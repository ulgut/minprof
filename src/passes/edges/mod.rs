//! Pass 2 — reference edge extraction.
//!
//! Streams the HPROF file a second time, extracting reference edges inline in
//! the parser thread with zero per-object allocation. Each InstanceDump,
//! ObjectArrayDump, and ClassDump is parsed directly from the 64 MiB work
//! buffer; the only allocation is the pooled `Vec<RawEdge>` batch that is
//! handed off to the main thread.
//!
//! Edges are accumulated in sorted chunks and merged into a single
//! `edges.bin` file sorted by `from_id`, forming a disk-backed forward
//! adjacency list for the dominator pass.

use std::collections::{BinaryHeap, HashMap};
use std::cmp::Reverse;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use anyhow::{Context, Result};

use crate::parser::gc_record::FieldType;
use crate::parser::record_stream_parser::{process_with_extractor, read_header};
use crate::passes::index::{ClassDescriptor, Pass1Output};

// ── On-disk edge format ──────────────────────────────────────────────────────
//
//  [0..8]  from_id: u64  (little-endian)
//  [8..16] to_id:   u64  (little-endian)

pub const EDGE_SIZE: usize = 16;
pub type RawEdge = [u8; EDGE_SIZE];

fn encode_edge(from: u64, to: u64) -> RawEdge {
    let mut buf = [0u8; EDGE_SIZE];
    buf[0..8].copy_from_slice(&from.to_le_bytes());
    buf[8..16].copy_from_slice(&to.to_le_bytes());
    buf
}

fn edge_from(e: &RawEdge) -> u64 {
    u64::from_le_bytes(e[0..8].try_into().unwrap())
}

fn edge_to(e: &RawEdge) -> u64 {
    u64::from_le_bytes(e[8..16].try_into().unwrap())
}

// ── External sorter ──────────────────────────────────────────────────────────

/// Above this many chunks, perform a two-level merge to cap the final fan-in.
const MAX_MERGE_FAN_IN: usize = 64;

struct EdgeSorter {
    output_dir: PathBuf,
    chunk_paths: Vec<PathBuf>,
    current: Vec<RawEdge>,
    edges_per_chunk: usize,
}

impl EdgeSorter {
    fn new(output_dir: PathBuf) -> Self {
        let chunk_bytes = crate::passes::sort_chunk_bytes();
        let edges_per_chunk = chunk_bytes / EDGE_SIZE;
        eprintln!(
            "  sort buffer: {:.1} GiB ({} edges/chunk)",
            chunk_bytes as f64 / (1 << 30) as f64,
            edges_per_chunk
        );
        Self {
            output_dir,
            chunk_paths: Vec::new(),
            current: Vec::with_capacity(edges_per_chunk),
            edges_per_chunk,
        }
    }

    fn push(&mut self, edge: RawEdge) -> Result<()> {
        self.current.push(edge);
        if self.current.len() >= self.edges_per_chunk {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        // Parallel sort + dedup within chunk.
        use rayon::slice::ParallelSliceMut;
        self.current.par_sort_unstable_by_key(|e| (edge_from(e), edge_to(e)));
        self.current.dedup();

        let path = self
            .output_dir
            .join(format!("edge_chunk_{}.bin", self.chunk_paths.len()));
        let mut w = BufWriter::new(File::create(&path).context("create edge chunk")?);
        for edge in self.current.drain(..) {
            w.write_all(&edge)?;
        }
        w.flush()?;
        self.chunk_paths.push(path);
        eprintln!("  flushed edge chunk {} ({} total)", self.chunk_paths.len(), self.chunk_paths.len());
        Ok(())
    }

    fn finish(mut self, output_path: &Path) -> Result<u64> {
        // Fast path: no chunks have been flushed to disk — sort in-memory and
        // write directly. Avoids one temporary file write + read cycle.
        if self.chunk_paths.is_empty() {
            if self.current.is_empty() {
                File::create(output_path).context("create empty edge file")?;
                return Ok(0);
            }
            use rayon::slice::ParallelSliceMut;
            eprintln!("  sorting {} edges in-memory (no chunk files needed)…", self.current.len());
            self.current.par_sort_unstable_by_key(|e| (edge_from(e), edge_to(e)));
            self.current.dedup();
            let count = self.current.len() as u64;
            let mut w = BufWriter::new(
                File::create(output_path).context("create edge file")?,
            );
            for edge in self.current.drain(..) {
                w.write_all(&edge)?;
            }
            w.flush()?;
            return Ok(count);
        }

        self.flush_chunk()?;

        let chunks = std::mem::take(&mut self.chunk_paths);

        match chunks.len() {
            0 => {
                File::create(output_path).context("create empty edge file")?;
                return Ok(0);
            }
            1 => {
                std::fs::rename(&chunks[0], output_path)
                    .context("rename single edge chunk")?;
            }
            n if n <= MAX_MERGE_FAN_IN => {
                eprintln!("  merging {} edge chunks…", n);
                merge_chunks(&chunks, output_path)?;
                for p in &chunks { let _ = std::fs::remove_file(p); }
            }
            n => {
                let group_size = MAX_MERGE_FAN_IN;
                let num_groups = (n + group_size - 1) / group_size;
                eprintln!("  two-level edge merge: {} chunks → {} groups…", n, num_groups);

                let mut intermediates: Vec<PathBuf> = Vec::with_capacity(num_groups);
                for (g, group) in chunks.chunks(group_size).enumerate() {
                    let inter = self.output_dir.join(format!("edge_inter_{g}.bin"));
                    eprintln!("    merging group {}/{} ({} chunks)…", g + 1, num_groups, group.len());
                    merge_chunks(group, &inter)?;
                    for p in group { let _ = std::fs::remove_file(p); }
                    intermediates.push(inter);
                }

                eprintln!("  final edge merge of {} intermediate files…", intermediates.len());
                merge_chunks(&intermediates, output_path)?;
                for p in &intermediates { let _ = std::fs::remove_file(p); }
            }
        }

        let count = std::fs::metadata(output_path)?.len() / EDGE_SIZE as u64;
        Ok(count)
    }
}

/// Clean up chunk files if the sorter is dropped before `finish()` (e.g. on panic).
impl Drop for EdgeSorter {
    fn drop(&mut self) {
        for p in &self.chunk_paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn merge_chunks(chunk_paths: &[PathBuf], output_path: &Path) -> Result<()> {
    let mut readers: Vec<BufReader<File>> = chunk_paths
        .iter()
        .map(|p| Ok(BufReader::new(File::open(p).context("open edge chunk")?)))
        .collect::<Result<_>>()?;

    // Sort by (from, to, chunk_index) so identical edges from different chunks
    // are adjacent and can be deduplicated by the last_edge check below.
    let mut heap: BinaryHeap<Reverse<(u64, u64, usize)>> = BinaryHeap::new();
    let mut peek: Vec<Option<RawEdge>> = vec![None; readers.len()];

    for (i, r) in readers.iter_mut().enumerate() {
        if let Some(e) = read_edge(r)? {
            heap.push(Reverse((edge_from(&e), edge_to(&e), i)));
            peek[i] = Some(e);
        }
    }

    let mut w = BufWriter::new(File::create(output_path).context("create merged edge file")?);
    let mut last_edge: Option<RawEdge> = None;
    while let Some(Reverse((_, _, idx))) = heap.pop() {
        let edge = peek[idx].take().unwrap();
        // Skip duplicate (from, to) pairs that appear across chunk boundaries.
        if last_edge.as_ref() != Some(&edge) {
            w.write_all(&edge)?;
            last_edge = Some(edge);
        }
        if let Some(next) = read_edge(&mut readers[idx])? {
            heap.push(Reverse((edge_from(&next), edge_to(&next), idx)));
            peek[idx] = Some(next);
        }
    }
    w.flush()?;
    Ok(())
}

fn read_edge(r: &mut impl Read) -> Result<Option<RawEdge>> {
    let mut buf = [0u8; EDGE_SIZE];
    match r.read_exact(&mut buf) {
        Ok(()) => Ok(Some(buf)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e).context("read edge chunk"),
    }
}

// ── Reverse edge index ────────────────────────────────────────────────────────

/// Build `reverse_edges.bin` from the already-sorted `edges.bin`.
///
/// Stores `(to_id: u64, from_id: u64)` pairs sorted by `to_id`.
/// Binary-searching by `to_id` yields all objects that hold a reference to a
/// given target — i.e. the target's referrers. Used for path-to-GC-root queries.
pub fn build_reverse_edges(edges_path: &Path, output_dir: &Path) -> Result<PathBuf> {
    let output_path = output_dir.join("reverse_edges.bin");
    let mut sorter = EdgeSorter::new(output_dir.to_path_buf());

    let mut reader = BufReader::new(
        File::open(edges_path).context("open edges.bin for reverse build")?,
    );
    while let Some(e) = read_edge(&mut reader)? {
        // Swap (from, to) → (to, from) so the sorter orders by to_id.
        sorter.push(encode_edge(edge_to(&e), edge_from(&e)))?;
    }

    sorter.finish(&output_path)?;
    Ok(output_path)
}

// ── Pass 2 output ─────────────────────────────────────────────────────────────

pub struct Pass2Output {
    /// Sorted forward edge file: (from_id, to_id) pairs sorted by from_id.
    pub edges_path: PathBuf,
    /// Sorted reverse edge file: (to_id, from_id) pairs sorted by to_id.
    /// Binary-search by to_id to find all referrers of an object.
    pub reverse_edges_path: PathBuf,
    pub edge_count: u64,
}

// ── Inline edge extractor ─────────────────────────────────────────────────────

/// Stateful HPROF parser that extracts reference edges directly from the raw
/// byte stream with zero per-object allocation. Runs in the extractor thread.
struct EdgeStreamExtractor {
    id_size: usize,
    /// Precomputed per-class flat list of byte offsets (within the InstanceDump
    /// raw-data block) at which an Object-typed field lives. Covers the class's
    /// own fields AND all inherited fields from the full superclass chain, in
    /// HPROF layout order. Built once at construction time.
    ///
    /// Per-object extraction becomes: one HashMap lookup + a flat u32 scan.
    /// No superclass-chain walking or per-field FieldType comparison at runtime.
    field_offsets: HashMap<u64, Vec<u32>>,
    /// Bytes remaining in the current HEAP_DUMP / HEAP_DUMP_SEGMENT body.
    /// Zero means we are parsing outer (top-level) HPROF records.
    heap_dump_remaining: usize,
}

impl EdgeStreamExtractor {
    fn new(id_size: u32, class_index: &HashMap<u64, ClassDescriptor>) -> Self {
        let is = id_size as usize;
        let mut field_offsets: HashMap<u64, Vec<u32>> =
            HashMap::with_capacity(class_index.len());

        for &class_id in class_index.keys() {
            let mut offsets: Vec<u32> = Vec::new();
            let mut cursor: u32 = 0;
            let mut cur_id = class_id;

            while cur_id != 0 {
                let Some(desc) = class_index.get(&cur_id) else { break; };
                for field in &desc.instance_fields {
                    let field_bytes = field.field_type.byte_size(id_size);
                    if field.field_type == FieldType::Object {
                        offsets.push(cursor);
                    }
                    cursor += field_bytes;
                }
                cur_id = desc.super_id;
            }

            field_offsets.insert(class_id, offsets);
        }

        eprintln!(
            "  precomputed Object-field offsets for {} classes",
            field_offsets.len()
        );

        Self {
            id_size: is,
            field_offsets,
            heap_dump_remaining: 0,
        }
    }

    /// Walk `buf`, append edges to `out`, return bytes consumed from `buf`.
    /// Returns 0 if there is not enough data for even one complete record.
    fn extract(&mut self, buf: &[u8], out: &mut Vec<RawEdge>) -> usize {
        let mut pos = 0;

        loop {
            let rem = &buf[pos..];
            if rem.is_empty() {
                break;
            }

            if self.heap_dump_remaining == 0 {
                // Outer record: tag(1) + time_offset(4) + length(4) = 9 bytes min.
                if rem.len() < 9 {
                    break;
                }
                let tag    = rem[0];
                let length = u32::from_be_bytes(rem[5..9].try_into().unwrap()) as usize;
                match tag {
                    0x0C | 0x1C => {
                        // HEAP_DUMP | HEAP_DUMP_SEGMENT — enter GC sub-record mode.
                        self.heap_dump_remaining = length;
                        pos += 9;
                    }
                    0x2C => {
                        // HEAP_DUMP_END
                        pos += 9;
                    }
                    _ => {
                        // Skip tag(1) + time_offset(4) + len(4) + body(length).
                        let total = 9 + length;
                        if rem.len() < total {
                            break;
                        }
                        pos += total;
                    }
                }
            } else {
                // GC sub-record mode.
                let n = self.extract_gc(rem, out);
                if n == 0 {
                    break; // Incomplete — need more data.
                }
                self.heap_dump_remaining = self.heap_dump_remaining.saturating_sub(n);
                pos += n;
            }
        }

        pos
    }

    /// Parse one GC sub-record from `buf`, emit edges into `out`, and return
    /// bytes consumed. Returns 0 if `buf` does not hold a complete record.
    fn extract_gc(&self, buf: &[u8], out: &mut Vec<RawEdge>) -> usize {
        if buf.is_empty() {
            return 0;
        }
        let is  = self.id_size;
        let tag = buf[0];
        let data = &buf[1..];

        match tag {
            0x21 => {
                // TAG_GC_INSTANCE_DUMP
                // object_id(is) | stack_serial(4) | class_id(is) | data_size(4) | raw[data_size]
                let hdr = 2 * is + 8;
                if data.len() < hdr { return 0; }
                let object_id = read_id_raw(is, data);
                let class_id  = read_id_raw(is, &data[is + 4..]);
                let data_size = u32::from_be_bytes(data[2*is+4..2*is+8].try_into().unwrap()) as usize;
                let total = 1 + hdr + data_size;
                if buf.len() < total { return 0; }
                let raw = &data[hdr..hdr + data_size];
                self.extract_instance_edges(object_id, class_id, raw, out);
                total
            }
            0x22 => {
                // TAG_GC_OBJ_ARRAY_DUMP
                // object_id(is) | stack_serial(4) | num_elements(4) | element_class_id(is) | ids[num*is]
                let hdr = 2 * is + 8;
                if data.len() < hdr { return 0; }
                let object_id    = read_id_raw(is, data);
                let num_elements = u32::from_be_bytes(data[is+4..is+8].try_into().unwrap()) as usize;
                let payload = num_elements * is;
                let total = 1 + hdr + payload;
                if buf.len() < total { return 0; }
                let elem_data = &data[hdr..hdr + payload];
                for chunk in elem_data.chunks_exact(is) {
                    let to = read_id_raw(is, chunk);
                    if to != 0 {
                        out.push(encode_edge(object_id, to));
                    }
                }
                total
            }
            0x23 => {
                // TAG_GC_PRIM_ARRAY_DUMP
                // object_id(is) | stack_serial(4) | num_elements(4) | element_type(1) | data[...]
                let hdr = is + 9;
                if data.len() < hdr { return 0; }
                let num_elements = u32::from_be_bytes(data[is+4..is+8].try_into().unwrap()) as usize;
                let elem_type    = data[is + 8];
                let total = 1 + hdr + num_elements * field_type_size(elem_type, is);
                if buf.len() < total { return 0; }
                total // no object references in primitive arrays
            }
            0x20 => {
                // TAG_GC_CLASS_DUMP — parse to find static Object-field edges.
                match class_dump_size_and_edges(is, data, out) {
                    Some(body_size) => 1 + body_size,
                    None => 0, // Incomplete
                }
            }
            // Root records — no outgoing object references; just skip.
            0xFF => { // ROOT_UNKNOWN: object_id(is)
                if data.len() < is { return 0; }
                1 + is
            }
            0x01 => { // ROOT_JNI_GLOBAL: object_id(is) + jni_ref(is)
                if data.len() < 2 * is { return 0; }
                1 + 2 * is
            }
            0x02 | 0x03 => { // ROOT_JNI_LOCAL | ROOT_JAVA_FRAME: object_id(is) + thread_serial(4) + frame_num(4)
                if data.len() < is + 8 { return 0; }
                1 + is + 8
            }
            0x04 | 0x06 => { // ROOT_NATIVE_STACK | ROOT_THREAD_BLOCK: object_id(is) + thread_serial(4)
                if data.len() < is + 4 { return 0; }
                1 + is + 4
            }
            0x05 | 0x07 => { // ROOT_STICKY_CLASS | ROOT_MONITOR_USED: object_id(is)
                if data.len() < is { return 0; }
                1 + is
            }
            0x08 => { // ROOT_THREAD_OBJ: object_id(is) + thread_serial(4) + stack_serial(4)
                if data.len() < is + 8 { return 0; }
                1 + is + 8
            }
            x => panic!("unknown GC sub-record tag: 0x{x:02X}"),
        }
    }

    /// Extract all Object-typed field references from an InstanceDump's raw bytes.
    /// Uses the precomputed flat offset list — one HashMap lookup, no chain walking.
    #[inline]
    fn extract_instance_edges(&self, from: u64, class_id: u64, raw: &[u8], out: &mut Vec<RawEdge>) {
        let is = self.id_size;
        let Some(offsets) = self.field_offsets.get(&class_id) else { return; };
        for &off in offsets {
            let off = off as usize;
            if off + is > raw.len() { break; } // offsets are ascending; safe to stop
            let to = read_id_raw(is, &raw[off..]);
            if to != 0 {
                out.push(encode_edge(from, to));
            }
        }
    }
}

// ── Inline parser helpers ─────────────────────────────────────────────────────

/// Read a big-endian object ID from the start of `buf`. `is` must be 4 or 8.
#[inline(always)]
fn read_id_raw(is: usize, buf: &[u8]) -> u64 {
    if is == 8 {
        u64::from_be_bytes(buf[..8].try_into().unwrap())
    } else {
        u32::from_be_bytes(buf[..4].try_into().unwrap()) as u64
    }
}

/// Byte size of a field value given its type byte and the heap's id_size.
/// Matches `FieldType::byte_size` in `gc_record.rs`.
fn field_type_size(ty: u8, is: usize) -> usize {
    match ty {
        2       => is, // Object
        4 | 8   => 1,  // Bool, Byte
        5 | 9   => 2,  // Char, Short
        6 | 10  => 4,  // Float, Int
        7 | 11  => 8,  // Double, Long
        _ => panic!("unknown field type byte: {ty}"),
    }
}

/// Parse a ClassDump body (starting AFTER the tag byte), emit edges for
/// Object-typed static fields, and return the number of bytes consumed.
/// Returns `None` if `buf` does not contain a complete ClassDump.
fn class_dump_size_and_edges(is: usize, buf: &[u8], out: &mut Vec<RawEdge>) -> Option<usize> {
    // Fixed header layout (after tag):
    //   class_object_id(is) + stack_serial(4) + super_class_id(is)
    //   + class_loader_id(is) + signers_id(is) + protection_domain_id(is)
    //   + reserved_1(is) + reserved_2(is) + instance_size(4)
    // = 7 * is + 8 bytes
    let fixed = 7 * is + 8;
    if buf.len() < fixed + 2 { return None; } // +2 for cp_count

    let class_object_id = read_id_raw(is, buf);
    let mut pos = fixed;

    // Constant pool
    let cp_count = u16::from_be_bytes(buf[pos..pos+2].try_into().unwrap()) as usize;
    pos += 2;
    for _ in 0..cp_count {
        if buf.len() < pos + 3 { return None; } // cp_index(2) + type(1)
        pos += 2; // cp_index (discarded)
        let ty = buf[pos];
        pos += 1;
        let sz = field_type_size(ty, is);
        if buf.len() < pos + sz { return None; }
        pos += sz;
    }

    // Static fields
    if buf.len() < pos + 2 { return None; }
    let static_count = u16::from_be_bytes(buf[pos..pos+2].try_into().unwrap()) as usize;
    pos += 2;
    for _ in 0..static_count {
        if buf.len() < pos + is + 1 { return None; } // name_id(is) + type(1)
        let ty = buf[pos + is];
        let sz = field_type_size(ty, is);
        if buf.len() < pos + is + 1 + sz { return None; }
        if ty == 2 { // Object
            let to = read_id_raw(is, &buf[pos + is + 1..]);
            if to != 0 {
                out.push(encode_edge(class_object_id, to));
            }
        }
        pos += is + 1 + sz;
    }

    // Instance field descriptors — name_id(is) + type(1), no value.
    if buf.len() < pos + 2 { return None; }
    let instance_count = u16::from_be_bytes(buf[pos..pos+2].try_into().unwrap()) as usize;
    pos += 2;
    let descriptors_size = instance_count * (is + 1);
    if buf.len() < pos + descriptors_size { return None; }
    pos += descriptors_size;

    Some(pos)
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(path: &Path, pass1: &Pass1Output, output_dir: &Path) -> Result<Pass2Output> {
    let (header, _) = read_header(path)?;
    let id_size = header.id_size;

    let mut sorter = EdgeSorter::new(output_dir.to_path_buf());

    {
        // Precompute flat Object-field offsets from the class index (cheap: ~MBs).
        // The extractor owns this table and is moved into the parser thread.
        let mut extractor = EdgeStreamExtractor::new(id_size, &pass1.class_index);
        process_with_extractor(
            path,
            move |buf: &[u8], edges: &mut Vec<RawEdge>| -> usize {
                extractor.extract(buf, edges)
            },
            &mut |batch: &mut Vec<RawEdge>| {
                for &edge in batch.iter() {
                    sorter.push(edge).expect("edge sort write failed");
                }
            },
        )
        .context("pass 2 streaming")?;
    }

    let edges_path = output_dir.join("edges.bin");
    let edge_count = sorter.finish(&edges_path)?;
    let reverse_edges_path = build_reverse_edges(&edges_path, output_dir)?;
    Ok(Pass2Output { edges_path, reverse_edges_path, edge_count })
}
