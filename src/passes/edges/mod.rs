//! Pass 2 — reference edge extraction.
//!
//! Streams the HPROF file a second time with data mode enabled so that
//! [`GcRecord::InstanceDump`] carries raw field bytes and
//! [`GcRecord::ObjectArrayDump`] carries decoded element IDs.
//!
//! For each object we walk its field descriptors (and those of its superclasses)
//! to find Object-typed fields, then emit a directed edge (from_id → to_id) for
//! every non-null reference found.
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

use crate::parser::gc_record::{FieldType, GcRecord};
use crate::parser::record::Record;
use crate::parser::record_stream_parser::{RecordVisitor, process_with_data, read_header};
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

const SORT_CHUNK_BYTES: usize = 256 * 1024 * 1024; // 256 MB
const EDGES_PER_CHUNK: usize = SORT_CHUNK_BYTES / EDGE_SIZE;

struct EdgeSorter {
    output_dir: PathBuf,
    chunk_paths: Vec<PathBuf>,
    current: Vec<RawEdge>,
}

impl EdgeSorter {
    fn new(output_dir: PathBuf) -> Self {
        Self {
            output_dir,
            chunk_paths: Vec::new(),
            current: Vec::with_capacity(EDGES_PER_CHUNK),
        }
    }

    fn push(&mut self, edge: RawEdge) -> Result<()> {
        self.current.push(edge);
        if self.current.len() >= EDGES_PER_CHUNK {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        // Sort by (from, to) so consecutive duplicates can be removed in one pass.
        self.current.sort_unstable_by_key(|e| (edge_from(e), edge_to(e)));
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
        Ok(())
    }

    fn finish(mut self, output_path: &Path) -> Result<u64> {
        self.flush_chunk()?;

        match self.chunk_paths.len() {
            0 => {
                File::create(output_path).context("create empty edge file")?;
                return Ok(0);
            }
            1 => {
                std::fs::rename(&self.chunk_paths[0], output_path)
                    .context("rename single edge chunk")?;
            }
            _ => {
                merge_chunks(&self.chunk_paths, output_path)?;
                for p in &self.chunk_paths {
                    let _ = std::fs::remove_file(p);
                }
            }
        }

        let count = std::fs::metadata(output_path)?.len() / EDGE_SIZE as u64;
        Ok(count)
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

// ── Visitor ───────────────────────────────────────────────────────────────────

pub struct EdgeVisitor<'a> {
    id_size: u32,
    class_index: &'a HashMap<u64, ClassDescriptor>,
    sorter: EdgeSorter,
    edge_count: u64,
}

impl<'a> EdgeVisitor<'a> {
    pub fn new(id_size: u32, class_index: &'a HashMap<u64, ClassDescriptor>, output_dir: &Path) -> Self {
        Self {
            id_size,
            class_index,
            sorter: EdgeSorter::new(output_dir.to_path_buf()),
            edge_count: 0,
        }
    }

    fn emit(&mut self, from: u64, to: u64) -> Result<()> {
        if to == 0 {
            return Ok(()); // null reference — skip
        }
        self.sorter.push(encode_edge(from, to))?;
        self.edge_count += 1;
        Ok(())
    }

    /// Extract all object references from an instance's raw field bytes.
    ///
    /// Field layout: the class's own fields come first in the byte stream,
    /// followed by the superclass's fields, and so on up the hierarchy.
    /// This matches HotSpot's HPROF output order.
    fn extract_instance_edges(&mut self, from: u64, class_id: u64, raw: &[u8]) -> Result<()> {
        let mut cursor = 0usize;
        let mut cur_id = class_id;

        while cur_id != 0 {
            let Some(desc) = self.class_index.get(&cur_id) else {
                // Unknown class — can't safely interpret remaining bytes.
                break;
            };

            for field in &desc.instance_fields {
                let field_bytes = field.field_type.byte_size(self.id_size) as usize;
                if cursor + field_bytes > raw.len() {
                    return Ok(()); // truncated or misaligned — stop safely
                }

                if field.field_type == FieldType::Object {
                    let to = read_id_be(self.id_size, &raw[cursor..]);
                    self.emit(from, to)?;
                }

                cursor += field_bytes;
            }

            cur_id = desc.super_id;
        }

        Ok(())
    }

    pub fn into_output(self, output_dir: &Path) -> Result<Pass2Output> {
        let edges_path = output_dir.join("edges.bin");
        let edge_count = self.sorter.finish(&edges_path)?;
        let reverse_edges_path = build_reverse_edges(&edges_path, output_dir)?;
        Ok(Pass2Output { edges_path, reverse_edges_path, edge_count })
    }

    fn handle_gc(&mut self, gc: GcRecord) {
        let result = match gc {
            GcRecord::InstanceDump { object_id, class_id, raw_data, .. } => {
                if !raw_data.is_empty() {
                    self.extract_instance_edges(object_id, class_id, &raw_data)
                } else {
                    Ok(())
                }
            }
            GcRecord::ObjectArrayDump { object_id, elements, .. } => {
                let mut r = Ok(());
                for to in elements {
                    if let Err(e) = self.emit(object_id, to) {
                        r = Err(e);
                        break;
                    }
                }
                r
            }
            GcRecord::ClassDump(fields) => {
                // Emit edges for Object-typed static fields on the class object.
                let mut r = Ok(());
                for (fi, fv) in &fields.static_fields {
                    if fi.field_type == FieldType::Object {
                        if let crate::parser::gc_record::FieldValue::Object(to_id) = fv {
                            if let Err(e) = self.emit(fields.class_object_id, *to_id) {
                                r = Err(e);
                                break;
                            }
                        }
                    }
                }
                r
            }
            // PrimitiveArrayDump, roots — no outgoing object references.
            _ => Ok(()),
        };
        result.expect("edge write failed");
    }
}

impl<'a> RecordVisitor for EdgeVisitor<'a> {
    fn on_record(&mut self, record: Record) {
        if let Record::GcSegment(gc) = record {
            self.handle_gc(gc);
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Read a big-endian object ID from a byte slice.
/// HPROF stores all integers in network (big-endian) byte order.
fn read_id_be(id_size: u32, buf: &[u8]) -> u64 {
    if id_size == 4 {
        u32::from_be_bytes(buf[0..4].try_into().unwrap()) as u64
    } else {
        u64::from_be_bytes(buf[0..8].try_into().unwrap())
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(path: &Path, pass1: &Pass1Output, output_dir: &Path) -> Result<Pass2Output> {
    let (header, _) = read_header(path)?;
    let mut visitor = EdgeVisitor::new(header.id_size, &pass1.class_index, output_dir);
    process_with_data(path, &mut visitor).context("pass 2 streaming")?;
    visitor.into_output(output_dir)
}
