//! Pass 1 — streaming index build.
//!
//! Streams the HPROF file once via [`IndexStreamExtractor`] (inline byte scanner
//! running in the parser thread) and produces:
//! - [`ClassDescriptor`] map in memory (needed for pass 2 field interpretation)
//! - Sorted object index on disk: `(object_id, class_id, shallow_size)` per object
//! - GC root ID list on disk

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;

use anyhow::{Context, Result};

use crate::parser::gc_record::{FieldInfo, FieldType};
use crate::parser::primitive_parsers::read_id_be;
use crate::parser::record_stream_parser::{process_with_extractor, read_header};
use crate::passes::{IO_BUF_SIZE, MAX_MERGE_FAN_IN};

// ── On-disk entry format ─────────────────────────────────────────────────────
//
// Each object entry is 20 bytes, little-endian:
//   [0..8]  object_id:    u64
//   [8..16] class_id:     u64
//  [16..20] shallow_size: u32

pub const ENTRY_SIZE: usize = 20;
pub type RawEntry = [u8; ENTRY_SIZE];

// ── Synthetic class IDs stored in object_index.bin ───────────────────────────
//
// Real heap addresses are always ≥ 0x1000 on any JVM, so values 0x01..0x0B are
// safe to use as synthetic identifiers for objects that have no Java class_id.

/// Class object — java.lang.Class (from ClassDump records).
pub const CLASS_ID_JAVA_CLASS: u64 = 0x01;
/// boolean[] primitive array.
pub const CLASS_ID_BOOL_ARRAY: u64 = 0x04; // FieldType::Bool   = 4
/// char[] primitive array.
pub const CLASS_ID_CHAR_ARRAY: u64 = 0x05; // FieldType::Char   = 5
/// float[] primitive array.
pub const CLASS_ID_FLOAT_ARRAY: u64 = 0x06; // FieldType::Float  = 6
/// double[] primitive array.
pub const CLASS_ID_DOUBLE_ARRAY: u64 = 0x07; // FieldType::Double = 7
/// byte[] primitive array.
pub const CLASS_ID_BYTE_ARRAY: u64 = 0x08; // FieldType::Byte   = 8
/// short[] primitive array.
pub const CLASS_ID_SHORT_ARRAY: u64 = 0x09; // FieldType::Short  = 9
/// int[] primitive array.
pub const CLASS_ID_INT_ARRAY: u64 = 0x0A; // FieldType::Int    = 10
/// long[] primitive array.
pub const CLASS_ID_LONG_ARRAY: u64 = 0x0B; // FieldType::Long   = 11

/// Flag ORed into the class_id field of ObjectArrayDump entries.
/// Bit 63 is always 0 for real heap addresses on x86_64, so this is safe.
pub const OBJECT_ARRAY_FLAG: u64 = 1u64 << 63;

pub fn encode_entry(object_id: u64, class_id: u64, shallow_size: u32) -> RawEntry {
    let mut buf = [0u8; ENTRY_SIZE];
    buf[0..8].copy_from_slice(&object_id.to_le_bytes());
    buf[8..16].copy_from_slice(&class_id.to_le_bytes());
    buf[16..20].copy_from_slice(&shallow_size.to_le_bytes());
    buf
}

pub fn decode_entry(buf: &RawEntry) -> (u64, u64, u32) {
    let object_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let class_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let shallow_size = u32::from_le_bytes(buf[16..20].try_into().unwrap());
    (object_id, class_id, shallow_size)
}

fn entry_id(buf: &RawEntry) -> u64 {
    u64::from_le_bytes(buf[0..8].try_into().unwrap())
}

// ── ClassDescriptor ──────────────────────────────────────────────────────────

/// Type alias for the in-memory class map used throughout the analysis.
pub type ClassDescriptorMap = std::collections::HashMap<u64, ClassDescriptor>;

/// Everything retained about a class after pass 1.
/// Lives in memory for the duration of the analysis — typically tens of MB.
#[derive(Clone)]
pub struct ClassDescriptor {
    /// Dot-separated class name, e.g. `java.lang.String`.
    pub name: String,
    /// Class object ID of the immediate superclass (0 if none).
    pub super_id: u64,
    /// Shallow size of one instance in bytes (from ClassDump's `instance_size`).
    pub instance_size: u32,
    /// Instance field descriptors declared directly on this class.
    /// Does NOT include inherited fields — walk the superclass chain in pass 2.
    pub instance_fields: Vec<FieldInfo>,
}

// ── Pass1Output ──────────────────────────────────────────────────────────────

pub struct Pass1Output {
    /// In-memory class index: class_object_id → ClassDescriptor.
    pub class_index: ClassDescriptorMap,
    /// GC root object IDs (de-duplicated).
    pub roots: Vec<u64>,
    /// Total number of objects indexed.
    pub object_count: u64,
    /// Path to the sorted object index file on disk.
    pub object_index_path: PathBuf,
    /// Path to compact shallow sizes file: one `u32` per object, same order
    /// as `object_index.bin`.
    pub shallow_sizes_path: PathBuf,
}

// ── External sorter ──────────────────────────────────────────────────────────

struct ExternalSorter {
    output_dir: PathBuf,
    chunk_paths: Vec<PathBuf>,
    current: Vec<RawEntry>,
    entries_per_chunk: usize,
    /// Monotonically increasing chunk counter — used for filenames so that
    /// background flushes that haven't been collected yet still get unique names.
    chunk_count: usize,
    /// In-flight background sort+write task. At most one pending at a time;
    /// collected (joined) at the start of each subsequent `flush_chunk` call.
    pending_flush: Option<thread::JoinHandle<Result<PathBuf>>>,
}

impl ExternalSorter {
    fn new(output_dir: PathBuf) -> Self {
        let chunk_bytes = crate::passes::sort_chunk_bytes();
        let entries_per_chunk = chunk_bytes / ENTRY_SIZE;
        eprintln!(
            "  sort buffer: {:.1} GiB ({} entries/chunk)",
            chunk_bytes as f64 / (1 << 30) as f64,
            entries_per_chunk
        );
        Self {
            output_dir,
            chunk_paths: Vec::new(),
            current: Vec::with_capacity(entries_per_chunk),
            entries_per_chunk,
            chunk_count: 0,
            pending_flush: None,
        }
    }

    fn push(&mut self, entry: RawEntry) -> Result<()> {
        self.current.push(entry);
        if self.current.len() >= self.entries_per_chunk {
            self.flush_chunk()?;
        }
        Ok(())
    }

    /// Join the in-flight sort+write thread (if any) and record its output path.
    fn collect_pending(&mut self) -> Result<()> {
        if let Some(handle) = self.pending_flush.take() {
            let path = handle.join().expect("sort-flush thread panicked")?;
            self.chunk_paths.push(path);
        }
        Ok(())
    }

    /// Sort the current buffer and write it to disk on a background thread,
    /// overlapping the write with continued parsing on the main thread.
    fn flush_chunk(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        // Join the previous background flush before starting a new one.
        self.collect_pending()?;

        let chunk_idx = self.chunk_count;
        self.chunk_count += 1;
        let out_path = self
            .output_dir
            .join(format!("object_index_chunk_{chunk_idx}.bin"));

        // Swap out the full buffer for a fresh one so parsing can continue
        // immediately while the old buffer is sorted and written in a thread.
        let to_sort = std::mem::replace(
            &mut self.current,
            Vec::with_capacity(self.entries_per_chunk),
        );

        let handle = thread::Builder::new()
            .name(format!("sort-flush-{chunk_idx}"))
            .spawn(move || -> Result<PathBuf> {
                use rayon::slice::ParallelSliceMut;
                let mut buf = to_sort;
                buf.par_sort_unstable_by_key(entry_id);
                let mut w = BufWriter::with_capacity(
                    IO_BUF_SIZE,
                    File::create(&out_path).context("create sort chunk")?,
                );
                // Safety: RawEntry = [u8; ENTRY_SIZE] — plain bytes, alignment 1, no padding.
                let bytes = unsafe {
                    std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), buf.len() * ENTRY_SIZE)
                };
                w.write_all(bytes)?;
                w.flush()?;
                eprintln!("  chunk {} written", chunk_idx + 1);
                Ok(out_path)
            })?;

        self.pending_flush = Some(handle);
        Ok(())
    }

    /// Merge all chunks into a single sorted file at `output_path`, applying
    /// `fixup` to every entry as it is written.
    ///
    /// Returns the total number of entries written.
    fn finish<F>(mut self, output_path: &Path, mut fixup: F) -> Result<u64>
    where
        F: FnMut(&mut RawEntry),
    {
        // Collect any in-flight background flush first.
        self.collect_pending()?;

        // Fast path: no chunks flushed — all data still lives in `current`.
        if self.chunk_paths.is_empty() {
            if self.current.is_empty() {
                File::create(output_path).context("create empty object index")?;
                return Ok(0);
            }
            use rayon::slice::ParallelSliceMut;
            let count = self.current.len() as u64;
            eprintln!("  sorting {count} entries in-memory (no chunk files needed)…");
            self.current.par_sort_unstable_by_key(entry_id);
            let mut w = BufWriter::with_capacity(
                IO_BUF_SIZE,
                File::create(output_path).context("create object index")?,
            );
            for mut entry in self.current.drain(..) {
                fixup(&mut entry);
                w.write_all(&entry)?;
            }
            w.flush()?;
            return Ok(count);
        }

        // Flush any remaining in-memory entries, then collect that flush.
        self.flush_chunk()?;
        self.collect_pending()?;

        // Take ownership so Drop sees an empty list and won't double-delete.
        let chunks = std::mem::take(&mut self.chunk_paths);

        match chunks.len() {
            0 => unreachable!(),
            n if n <= MAX_MERGE_FAN_IN => {
                eprintln!("  merging {} chunks…", n);
                merge_chunks_with_fixup(&chunks, output_path, fixup)?;
                for p in &chunks {
                    let _ = std::fs::remove_file(p);
                }
            }
            n => {
                // Two-level merge: intermediate files written without fixup;
                // fixup applied during the final merge pass.
                let group_size = MAX_MERGE_FAN_IN;
                let num_groups = (n + group_size - 1) / group_size;
                eprintln!("  two-level merge: {} chunks → {} groups…", n, num_groups);

                let mut intermediates: Vec<PathBuf> = Vec::with_capacity(num_groups);
                for (g, group) in chunks.chunks(group_size).enumerate() {
                    let inter = self.output_dir.join(format!("object_index_inter_{g}.bin"));
                    eprintln!(
                        "    merging group {}/{} ({} chunks)…",
                        g + 1,
                        num_groups,
                        group.len()
                    );
                    merge_chunks_with_fixup(group, &inter, |_| {})?;
                    for p in group {
                        let _ = std::fs::remove_file(p);
                    }
                    intermediates.push(inter);
                }

                eprintln!(
                    "  final merge of {} intermediate files…",
                    intermediates.len()
                );
                merge_chunks_with_fixup(&intermediates, output_path, fixup)?;
                for p in &intermediates {
                    let _ = std::fs::remove_file(p);
                }
            }
        }

        let count = std::fs::metadata(output_path)?.len() / ENTRY_SIZE as u64;
        Ok(count)
    }
}

/// Clean up any chunk files if the sorter is dropped before `finish()` (e.g. on panic).
impl Drop for ExternalSorter {
    fn drop(&mut self) {
        // Join in-flight flush so we can retrieve its file path and delete it.
        if let Some(handle) = self.pending_flush.take() {
            if let Ok(Ok(path)) = handle.join() {
                self.chunk_paths.push(path);
            }
        }
        for p in &self.chunk_paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// K-way merge of sorted chunk files into a single sorted output file.
/// `fixup` is called on each entry immediately before it is written.
fn merge_chunks_with_fixup<F>(
    chunk_paths: &[PathBuf],
    output_path: &Path,
    mut fixup: F,
) -> Result<()>
where
    F: FnMut(&mut RawEntry),
{
    let per_reader_buf = (IO_BUF_SIZE / chunk_paths.len().max(1)).max(256 * 1024);
    let mut readers: Vec<BufReader<File>> = chunk_paths
        .iter()
        .map(|p| {
            Ok(BufReader::with_capacity(
                per_reader_buf,
                File::open(p).context("open sort chunk")?,
            ))
        })
        .collect::<Result<_>>()?;

    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
    let mut peek: Vec<Option<RawEntry>> = vec![None; readers.len()];

    for (i, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = read_entry(reader)? {
            heap.push(Reverse((entry_id(&entry), i)));
            peek[i] = Some(entry);
        }
    }

    let mut w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(output_path).context("create merged object index")?,
    );

    while let Some(Reverse((_, idx))) = heap.pop() {
        let mut entry = peek[idx].take().unwrap();
        fixup(&mut entry);
        w.write_all(&entry)?;

        if let Some(next) = read_entry(&mut readers[idx])? {
            heap.push(Reverse((entry_id(&next), idx)));
            peek[idx] = Some(next);
        }
    }

    w.flush()?;
    Ok(())
}

fn read_entry(reader: &mut impl Read) -> Result<Option<RawEntry>> {
    let mut buf = [0u8; ENTRY_SIZE];
    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(buf)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e).context("read sort chunk entry"),
    }
}

// ── IndexItem ─────────────────────────────────────────────────────────────────

/// Items emitted by the extractor thread and consumed on the main thread.
enum IndexItem {
    Entry(RawEntry),
    Root(u64),
    Utf8String {
        id: u64,
        value: String,
    },
    LoadClass {
        class_object_id: u64,
        class_name_id: u64,
    },
    ClassDump {
        class_object_id: u64,
        super_id: u64,
        instance_size: u32,
        /// Sum of static field data sizes — used to compute the class object's shallow size.
        static_shallow_size: u32,
        instance_fields: Vec<FieldInfo>,
    },
}

// ── IndexStreamExtractor ──────────────────────────────────────────────────────

/// Inline byte-scanner state.  Moved into the extractor closure and runs on the
/// parser thread alongside the reader thread, avoiding all nom overhead.
struct IndexStreamExtractor {
    id_size: usize,
    /// Bytes remaining in the current HEAP_DUMP / HEAP_DUMP_SEGMENT body.
    /// When zero we are between top-level records.
    heap_dump_remaining: usize,
}

impl IndexStreamExtractor {
    /// Scan `buf` (the unconsumed tail of the work buffer), appending items to
    /// `batch`.  Returns the number of bytes consumed from `buf`.
    fn extract(&mut self, buf: &[u8], batch: &mut Vec<IndexItem>) -> usize {
        let mut pos = 0;
        loop {
            let advanced = if self.heap_dump_remaining == 0 {
                self.try_outer(buf, &mut pos, batch)
            } else {
                self.try_gc(buf, &mut pos, batch)
            };
            if !advanced {
                break;
            }
        }
        pos
    }

    /// Try to parse one outer (top-level) HPROF record starting at `buf[*pos]`.
    /// Returns true and advances `*pos` on success; returns false if more data needed.
    fn try_outer(&mut self, buf: &[u8], pos: &mut usize, batch: &mut Vec<IndexItem>) -> bool {
        let avail = buf.len() - *pos;
        // Outer record header: tag(1) + time_offset(4) + body_length(4) = 9 bytes.
        if avail < 9 {
            return false;
        }
        let tag = buf[*pos];
        let length = u32::from_be_bytes(buf[*pos + 5..*pos + 9].try_into().unwrap()) as usize;

        match tag {
            0x01 => {
                // UTF8_STRING: body = id(is) + utf8_bytes[length - is]
                if avail < 9 + length {
                    return false;
                }
                let body = &buf[*pos + 9..*pos + 9 + length];
                let id = read_id_be(self.id_size, body);
                let value = String::from_utf8_lossy(&body[self.id_size..]).into_owned();
                batch.push(IndexItem::Utf8String { id, value });
                *pos += 9 + length;
                true
            }
            0x02 => {
                // LOAD_CLASS: body = serial(4) + class_id(is) + stack_serial(4) + name_id(is)
                if avail < 9 + length {
                    return false;
                }
                let body = &buf[*pos + 9..*pos + 9 + length];
                let class_object_id = read_id_be(self.id_size, &body[4..]);
                let class_name_id = read_id_be(self.id_size, &body[4 + self.id_size + 4..]);
                batch.push(IndexItem::LoadClass {
                    class_object_id,
                    class_name_id,
                });
                *pos += 9 + length;
                true
            }
            0x0C | 0x1C => {
                // HEAP_DUMP / HEAP_DUMP_SEGMENT — body is GC sub-records, not a blob.
                self.heap_dump_remaining = length;
                *pos += 9;
                true
            }
            _ => {
                // All other outer records: skip body entirely.
                if avail < 9 + length {
                    return false;
                }
                *pos += 9 + length;
                true
            }
        }
    }

    /// Try to parse one GC sub-record starting at `buf[*pos]` (within a heap dump).
    /// Returns true and advances `*pos` on success; returns false if more data needed.
    fn try_gc(&mut self, buf: &[u8], pos: &mut usize, batch: &mut Vec<IndexItem>) -> bool {
        let avail = buf.len() - *pos;
        if avail == 0 {
            return false;
        }
        let is = self.id_size;
        let tag = buf[*pos];
        // `data` is the slice starting immediately after the tag byte.
        let data = &buf[*pos + 1..];
        let data_avail = avail - 1;

        let consumed = match tag {
            0xFF => {
                // ROOT_UNKNOWN: object_id(is)
                if data_avail < is {
                    return false;
                }
                batch.push(IndexItem::Root(read_id_be(is, data)));
                1 + is
            }
            0x01 => {
                // ROOT_JNI_GLOBAL: object_id(is) + jni_ref_id(is)
                if data_avail < 2 * is {
                    return false;
                }
                batch.push(IndexItem::Root(read_id_be(is, data)));
                1 + 2 * is
            }
            0x02 | 0x03 => {
                // ROOT_JNI_LOCAL / ROOT_JAVA_FRAME: object_id(is) + thread(4) + frame(4)
                if data_avail < is + 8 {
                    return false;
                }
                batch.push(IndexItem::Root(read_id_be(is, data)));
                1 + is + 8
            }
            0x04 | 0x06 => {
                // ROOT_NATIVE_STACK / ROOT_THREAD_BLOCK: object_id(is) + thread(4)
                if data_avail < is + 4 {
                    return false;
                }
                batch.push(IndexItem::Root(read_id_be(is, data)));
                1 + is + 4
            }
            0x05 | 0x07 => {
                // ROOT_STICKY_CLASS / ROOT_MONITOR_USED: object_id(is)
                if data_avail < is {
                    return false;
                }
                batch.push(IndexItem::Root(read_id_be(is, data)));
                1 + is
            }
            0x08 => {
                // ROOT_THREAD_OBJ: object_id(is) + thread_serial(4) + stack_serial(4)
                if data_avail < is + 8 {
                    return false;
                }
                batch.push(IndexItem::Root(read_id_be(is, data)));
                1 + is + 8
            }
            0x20 => {
                // CLASS_DUMP — variable-length record
                match try_parse_class_dump(is, data) {
                    Some((n, item)) => {
                        batch.push(item);
                        1 + n
                    }
                    None => return false,
                }
            }
            0x21 => {
                // INSTANCE_DUMP: object_id(is) + stack(4) + class_id(is) + data_size(4) + data[data_size]
                let hdr = 2 * is + 8;
                if data_avail < hdr {
                    return false;
                }
                let oid = read_id_be(is, data);
                let class_id = read_id_be(is, &data[is + 4..]);
                let data_size =
                    u32::from_be_bytes(data[2 * is + 4..2 * is + 8].try_into().unwrap());
                let total = hdr + data_size as usize;
                if data_avail < total {
                    return false;
                }
                // Use data_size as placeholder; fixup corrects to instance_size in finish().
                batch.push(IndexItem::Entry(encode_entry(oid, class_id, data_size)));
                1 + total
            }
            0x22 => {
                // OBJ_ARRAY_DUMP: object_id(is) + stack(4) + num(4) + class_id(is) + elems[num*is]
                let hdr = 2 * is + 8;
                if data_avail < hdr {
                    return false;
                }
                let oid = read_id_be(is, data);
                let num = u32::from_be_bytes(data[is + 4..is + 8].try_into().unwrap());
                let class_id = read_id_be(is, &data[is + 8..]);
                let total = hdr + num as usize * is;
                if data_avail < total {
                    return false;
                }
                // JVM header (16) + array length field (4) + element references.
                let shallow = 16u32 + 4 + num.saturating_mul(is as u32);
                batch.push(IndexItem::Entry(encode_entry(
                    oid,
                    class_id | OBJECT_ARRAY_FLAG,
                    shallow,
                )));
                1 + total
            }
            0x23 => {
                // PRIM_ARRAY_DUMP: object_id(is) + stack(4) + num(4) + elem_type(1) + data
                let hdr = is + 9;
                if data_avail < hdr {
                    return false;
                }
                let oid = read_id_be(is, data);
                let num = u32::from_be_bytes(data[is + 4..is + 8].try_into().unwrap());
                let elem_type = FieldType::from_value(data[is + 8]);
                let elem_size = elem_type.byte_size(is as u32); // u32
                let total = hdr + num as usize * elem_size as usize;
                if data_avail < total {
                    return false;
                }
                let shallow = 16u32 + 4 + num.saturating_mul(elem_size);
                batch.push(IndexItem::Entry(encode_entry(
                    oid,
                    elem_type as u64,
                    shallow,
                )));
                1 + total
            }
            x => panic!("unknown GC sub-record tag: 0x{x:02X}"),
        };

        self.heap_dump_remaining = self.heap_dump_remaining.saturating_sub(consumed);
        *pos += consumed;
        true
    }
}

/// Try to parse a CLASS_DUMP sub-record from `buf` (the bytes immediately after
/// the 0x20 tag byte).  Returns `(bytes_consumed, IndexItem)` or `None` if there
/// is not enough data in the buffer.
fn try_parse_class_dump(is: usize, buf: &[u8]) -> Option<(usize, IndexItem)> {
    // Fixed header layout (all big-endian):
    //   class_id(is) + stack_serial(4) + super_id(is)
    //   + class_loader_id(is) + signers_id(is) + protection_domain_id(is)
    //   + reserved_1(is) + reserved_2(is) + instance_size(4)
    // = 7*is + 8 bytes, followed by cp_count(2).
    let fixed = 7 * is + 8;
    if buf.len() < fixed + 2 {
        return None;
    }

    let mut p = 0;
    let class_object_id = read_id_be(is, &buf[p..]);
    p += is;
    p += 4; // skip stack_serial
    let super_id = read_id_be(is, &buf[p..]);
    p += is;
    p += 5 * is; // skip class_loader_id, signers_id, prot_domain, reserved_1, reserved_2
    let instance_size = u32::from_be_bytes(buf[p..p + 4].try_into().unwrap());
    p += 4;

    // Constant pool — scan through and discard.
    let cp_count = u16::from_be_bytes(buf[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    for _ in 0..cp_count {
        // constant_pool_index(2) + type(1) + value
        if buf.len() < p + 3 {
            return None;
        }
        p += 2; // skip index
        let ty = FieldType::from_value(buf[p]);
        p += 1;
        let sz = ty.byte_size(is as u32) as usize;
        if buf.len() < p + sz {
            return None;
        }
        p += sz;
    }

    // Static fields — sum their data sizes for the class object's shallow size.
    if buf.len() < p + 2 {
        return None;
    }
    let static_count = u16::from_be_bytes(buf[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    let mut static_shallow_size: u32 = 0;
    for _ in 0..static_count {
        // name_id(is) + type(1) + value
        if buf.len() < p + is + 1 {
            return None;
        }
        p += is; // skip name_id
        let ty = FieldType::from_value(buf[p]);
        p += 1;
        let sz = ty.byte_size(is as u32) as usize;
        if buf.len() < p + sz {
            return None;
        }
        static_shallow_size += sz as u32;
        p += sz;
    }

    // Instance fields — kept for pass 2 reference extraction.
    if buf.len() < p + 2 {
        return None;
    }
    let instance_count = u16::from_be_bytes(buf[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    let mut instance_fields = Vec::with_capacity(instance_count);
    for _ in 0..instance_count {
        // name_id(is) + type(1)
        if buf.len() < p + is + 1 {
            return None;
        }
        let name_id = read_id_be(is, &buf[p..]);
        p += is;
        let ty = FieldType::from_value(buf[p]);
        p += 1;
        instance_fields.push(FieldInfo {
            name_id,
            field_type: ty,
        });
    }

    Some((
        p,
        IndexItem::ClassDump {
            class_object_id,
            super_id,
            instance_size,
            static_shallow_size,
            instance_fields,
        },
    ))
}

// ── class_names.bin serialisation ────────────────────────────────────────────
//
// Format (little-endian throughout):
//   u64: class_count
//   for each class (any order):
//     u64: class_id
//     u64: super_id
//     u32: name_len
//     u8[name_len]: class name (UTF-8, dot-separated)

fn write_class_names(class_index: &ClassDescriptorMap, path: &Path) -> Result<()> {
    let mut w = BufWriter::new(File::create(path).context("create class_names.bin")?);
    w.write_all(&(class_index.len() as u64).to_le_bytes())?;
    let mut entries: Vec<(&u64, &ClassDescriptor)> = class_index.iter().collect();
    entries.sort_unstable_by_key(|(id, _)| **id);
    for (&class_id, desc) in entries {
        w.write_all(&class_id.to_le_bytes())?;
        w.write_all(&desc.super_id.to_le_bytes())?;
        let name = desc.name.as_bytes();
        w.write_all(&(name.len() as u32).to_le_bytes())?;
        w.write_all(name)?;
    }
    w.flush()?;
    Ok(())
}

/// Deserialise `class_names.bin` into a `ClassDescriptorMap`.
///
/// `instance_fields` and `instance_size` are left empty/zero — sufficient for
/// the query phase (which only needs class names and super-class chains).
pub fn load_class_index(path: &Path) -> Result<ClassDescriptorMap> {
    let mut r = BufReader::new(File::open(path).context("open class_names.bin")?);

    let mut buf8 = [0u8; 8];
    let mut buf4 = [0u8; 4];

    r.read_exact(&mut buf8)?;
    let count = u64::from_le_bytes(buf8) as usize;

    let mut map = ClassDescriptorMap::with_capacity(count);
    for _ in 0..count {
        r.read_exact(&mut buf8)?;
        let class_id = u64::from_le_bytes(buf8);
        r.read_exact(&mut buf8)?;
        let super_id = u64::from_le_bytes(buf8);
        r.read_exact(&mut buf4)?;
        let name_len = u32::from_le_bytes(buf4) as usize;
        let mut name_bytes = vec![0u8; name_len];
        r.read_exact(&mut name_bytes)?;
        let name = String::from_utf8(name_bytes).context("class name UTF-8")?;
        map.insert(
            class_id,
            ClassDescriptor {
                name,
                super_id,
                instance_size: 0,
                instance_fields: Vec::new(),
            },
        );
    }
    Ok(map)
}

/// Load sorted object IDs from `object_index.bin`.
/// Position `i` in the returned Vec is the node index for that object ID.
pub fn load_object_ids(index_path: &Path) -> Result<Vec<u64>> {
    let file_len = std::fs::metadata(index_path)?.len() as usize;
    let n = file_len / ENTRY_SIZE;
    let mut ids = Vec::with_capacity(n);
    let mut reader = BufReader::with_capacity(
        IO_BUF_SIZE,
        File::open(index_path).context("open object index")?,
    );
    let mut buf = [0u8; ENTRY_SIZE];
    while reader.read_exact(&mut buf).is_ok() {
        ids.push(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
    }
    Ok(ids)
}

/// Load GC root IDs from `roots.bin`.
pub fn load_roots(path: &Path) -> Result<Vec<u64>> {
    let len = std::fs::metadata(path)?.len() as usize;
    let mut r = BufReader::new(File::open(path).context("open roots.bin")?);
    let mut roots = Vec::with_capacity(len / 8);
    let mut buf = [0u8; 8];
    loop {
        match r.read_exact(&mut buf) {
            Ok(()) => roots.push(u64::from_le_bytes(buf)),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e).context("read roots.bin"),
        }
    }
    Ok(roots)
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Run pass 1 over the HPROF file at `path`, writing index files into `output_dir`.
pub fn run(path: &Path, output_dir: &Path) -> Result<Pass1Output> {
    std::fs::create_dir_all(output_dir).context("create output directory")?;

    let (header, _) = read_header(path)?;
    let id_size = header.id_size as usize;

    let mut string_table: HashMap<u64, String> = HashMap::new();
    let mut name_id_map: HashMap<u64, u64> = HashMap::new();
    let mut class_index: HashMap<u64, ClassDescriptor> = HashMap::new();
    let mut roots: Vec<u64> = Vec::new();
    let mut sorter = ExternalSorter::new(output_dir.to_path_buf());

    let mut extractor = IndexStreamExtractor {
        id_size,
        heap_dump_remaining: 0,
    };

    process_with_extractor(
        path,
        move |buf, batch| extractor.extract(buf, batch),
        &mut |batch: &mut Vec<IndexItem>| {
            for item in batch.drain(..) {
                match item {
                    IndexItem::Utf8String { id, value } => {
                        string_table.insert(id, value);
                    }
                    IndexItem::LoadClass {
                        class_object_id,
                        class_name_id,
                    } => {
                        name_id_map.insert(class_object_id, class_name_id);
                    }
                    IndexItem::Root(oid) => {
                        roots.push(oid);
                    }
                    IndexItem::ClassDump {
                        class_object_id,
                        super_id,
                        instance_size,
                        static_shallow_size,
                        instance_fields,
                    } => {
                        // JVM object header (16) + static field data.
                        let shallow = 16u32 + static_shallow_size;
                        sorter
                            .push(encode_entry(class_object_id, CLASS_ID_JAVA_CLASS, shallow))
                            .expect("object index write failed");
                        class_index.insert(
                            class_object_id,
                            ClassDescriptor {
                                name: String::new(), // resolved below
                                super_id,
                                instance_size,
                                instance_fields,
                            },
                        );
                    }
                    IndexItem::Entry(entry) => {
                        sorter.push(entry).expect("object index write failed");
                    }
                }
            }
        },
    )
    .context("pass 1 streaming")?;

    // Resolve class names now that all Utf8String + LoadClass records have been seen.
    for (class_id, desc) in &mut class_index {
        if let Some(&name_sid) = name_id_map.get(class_id) {
            if let Some(raw) = string_table.get(&name_sid) {
                desc.name = if raw.contains('/') {
                    raw.replace('/', ".")
                } else {
                    raw.clone()
                };
            }
        }
    }

    // Write sorted object index + compact shallow_sizes.bin.
    //
    // The fixup closure patches shallow_size for InstanceDump entries:
    // `data_size` (raw field bytes, no header) is corrected to
    // `ClassDump.instance_size` (which includes the JVM object header).
    // It also writes each entry's shallow_size to a compact sidecar file,
    // saving pass 4 from re-reading the object index.
    let index_path = output_dir.join("object_index.bin");
    let shallow_sizes_path = output_dir.join("shallow_sizes.bin");
    let mut shallow_w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(&shallow_sizes_path).context("create shallow_sizes.bin")?,
    );
    let object_count = sorter.finish(&index_path, |entry| {
        let (_, class_id, _) = decode_entry(entry);
        if class_id & OBJECT_ARRAY_FLAG == 0 && class_id > CLASS_ID_LONG_ARRAY {
            if let Some(desc) = class_index.get(&class_id) {
                entry[16..20].copy_from_slice(&desc.instance_size.to_le_bytes());
            }
        }
        // Write the (possibly patched) shallow size to the sidecar file.
        shallow_w
            .write_all(&entry[16..20])
            .expect("write shallow_sizes.bin");
    })?;
    shallow_w.flush().context("flush shallow_sizes.bin")?;

    // Write roots (de-duplicated).
    let roots_path = output_dir.join("roots.bin");
    let mut rw = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(&roots_path).context("create roots file")?,
    );
    roots.sort_unstable();
    roots.dedup();
    for &id in &roots {
        rw.write_all(&id.to_le_bytes())?;
    }
    rw.flush()?;

    // Write class names for subsequent runs / query phase.
    let class_names_path = output_dir.join("class_names.bin");
    write_class_names(&class_index, &class_names_path).context("write class_names.bin")?;

    eprintln!(
        "  {} objects, {} classes, {} roots",
        object_count,
        class_index.len(),
        roots.len()
    );

    Ok(Pass1Output {
        class_index,
        roots,
        object_count,
        object_index_path: index_path,
        shallow_sizes_path,
    })
}
