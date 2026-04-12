//! Pass 1 — streaming index build.
//!
//! Streams the HPROF file once via [`RecordVisitor`] and produces:
//! - [`ClassDescriptor`] map in memory (needed for pass 2 field interpretation)
//! - Sorted object index on disk: `(object_id, class_id, shallow_size)` per object
//! - GC root ID list on disk

use std::collections::{BinaryHeap, HashMap};
use std::cmp::Reverse;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::parser::gc_record::{FieldInfo, FieldType, GcRecord};
use crate::parser::record::Record;
use crate::parser::record_stream_parser::{RecordVisitor, process, read_header};

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
pub const CLASS_ID_JAVA_CLASS:   u64 = 0x01;
/// boolean[] primitive array.
pub const CLASS_ID_BOOL_ARRAY:   u64 = 0x04; // FieldType::Bool   = 4
/// char[] primitive array.
pub const CLASS_ID_CHAR_ARRAY:   u64 = 0x05; // FieldType::Char   = 5
/// float[] primitive array.
pub const CLASS_ID_FLOAT_ARRAY:  u64 = 0x06; // FieldType::Float  = 6
/// double[] primitive array.
pub const CLASS_ID_DOUBLE_ARRAY: u64 = 0x07; // FieldType::Double = 7
/// byte[] primitive array.
pub const CLASS_ID_BYTE_ARRAY:   u64 = 0x08; // FieldType::Byte   = 8
/// short[] primitive array.
pub const CLASS_ID_SHORT_ARRAY:  u64 = 0x09; // FieldType::Short  = 9
/// int[] primitive array.
pub const CLASS_ID_INT_ARRAY:    u64 = 0x0A; // FieldType::Int    = 10
/// long[] primitive array.
pub const CLASS_ID_LONG_ARRAY:   u64 = 0x0B; // FieldType::Long   = 11

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

/// Everything retained about a class after pass 1.
/// Lives in memory for the duration of the analysis — typically tens of MB.
/// Type alias for the in-memory class map used throughout the analysis.
pub type ClassDescriptorMap = std::collections::HashMap<u64, ClassDescriptor>;

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
}

// ── External sorter ──────────────────────────────────────────────────────────

/// Above this many chunks, do a two-level merge (merge groups → then merge groups).
/// Keeps the final merge fan-in small regardless of dump size.
const MAX_MERGE_FAN_IN: usize = 64;

struct ExternalSorter {
    output_dir: PathBuf,
    chunk_paths: Vec<PathBuf>,
    current: Vec<RawEntry>,
    entries_per_chunk: usize,
}

impl ExternalSorter {
    fn new(output_dir: PathBuf) -> Self {
        // Compute chunk capacity once from physical-RAM detection.
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
            current: Vec::with_capacity(entries_per_chunk.min(64 * 1024 * 1024)),
            entries_per_chunk,
        }
    }

    fn push(&mut self, entry: RawEntry) -> Result<()> {
        self.current.push(entry);
        if self.current.len() >= self.entries_per_chunk {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        // Parallel sort via rayon — cuts sort time proportionally to core count.
        use rayon::slice::ParallelSliceMut;
        self.current.par_sort_unstable_by_key(entry_id);

        let path = self
            .output_dir
            .join(format!("object_index_chunk_{}.bin", self.chunk_paths.len()));
        let mut w = BufWriter::new(File::create(&path).context("create sort chunk")?);
        for entry in self.current.drain(..) {
            w.write_all(&entry)?;
        }
        w.flush()?;
        self.chunk_paths.push(path);
        eprintln!("  flushed chunk {} ({} total)", self.chunk_paths.len(), self.chunk_paths.len());
        Ok(())
    }

    /// Merge all chunks into a single sorted file at `output_path`.
    /// Uses a two-level merge when the chunk count exceeds MAX_MERGE_FAN_IN,
    /// keeping the final fan-in small regardless of dump size.
    /// Returns the total number of entries written.
    fn finish(mut self, output_path: &Path) -> Result<u64> {
        self.flush_chunk()?;

        // Take ownership of chunk_paths so Drop sees an empty list and doesn't
        // double-remove files we're about to handle here.
        let chunks = std::mem::take(&mut self.chunk_paths);

        match chunks.len() {
            0 => {
                File::create(output_path).context("create empty object index")?;
                return Ok(0);
            }
            1 => {
                std::fs::rename(&chunks[0], output_path)
                    .context("rename single sort chunk")?;
            }
            n if n <= MAX_MERGE_FAN_IN => {
                eprintln!("  merging {} chunks…", n);
                merge_chunks(&chunks, output_path)?;
                for p in &chunks { let _ = std::fs::remove_file(p); }
            }
            n => {
                // Two-level merge: group into batches of MAX_MERGE_FAN_IN,
                // merge each batch into an intermediate file, then final merge.
                let group_size = MAX_MERGE_FAN_IN;
                let num_groups = (n + group_size - 1) / group_size;
                eprintln!("  two-level merge: {} chunks → {} groups…", n, num_groups);

                let mut intermediates: Vec<PathBuf> = Vec::with_capacity(num_groups);
                for (g, group) in chunks.chunks(group_size).enumerate() {
                    let inter = self.output_dir.join(format!("object_index_inter_{g}.bin"));
                    eprintln!("    merging group {}/{} ({} chunks)…", g + 1, num_groups, group.len());
                    merge_chunks(group, &inter)?;
                    for p in group { let _ = std::fs::remove_file(p); }
                    intermediates.push(inter);
                }

                eprintln!("  final merge of {} intermediate files…", intermediates.len());
                merge_chunks(&intermediates, output_path)?;
                for p in &intermediates { let _ = std::fs::remove_file(p); }
            }
        }

        let count = std::fs::metadata(output_path)?.len() / ENTRY_SIZE as u64;
        Ok(count)
    }
}

/// Clean up any chunk files if the sorter is dropped before `finish()` (e.g. on panic).
impl Drop for ExternalSorter {
    fn drop(&mut self) {
        for p in &self.chunk_paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// K-way merge of sorted chunk files into a single sorted output file.
fn merge_chunks(chunk_paths: &[PathBuf], output_path: &Path) -> Result<()> {
    let mut readers: Vec<BufReader<File>> = chunk_paths
        .iter()
        .map(|p| Ok(BufReader::new(File::open(p).context("open sort chunk")?)))
        .collect::<Result<_>>()?;

    // Min-heap: (object_id, chunk_index). Reverse for min semantics.
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
    let mut peek: Vec<Option<RawEntry>> = vec![None; readers.len()];

    // Seed the heap with the first entry from each chunk.
    for (i, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = read_entry(reader)? {
            heap.push(Reverse((entry_id(&entry), i)));
            peek[i] = Some(entry);
        }
    }

    let mut w = BufWriter::new(File::create(output_path).context("create merged object index")?);

    while let Some(Reverse((_, idx))) = heap.pop() {
        let entry = peek[idx].take().unwrap();
        w.write_all(&entry)?;

        // Advance this chunk's reader.
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

// ── Instance size fixup ───────────────────────────────────────────────────────

/// Replace the `shallow_size` field for every InstanceDump entry in
/// `object_index.bin` with the authoritative value from `ClassDump.instance_size`.
///
/// InstanceDump entries are identified as those whose `class_id` is:
///   - not flagged as an object array (`OBJECT_ARRAY_FLAG` unset), and
///   - above the synthetic range (> `CLASS_ID_LONG_ARRAY` = 0x0B).
///
/// Writes to a temp file, then atomically replaces the original.
fn fixup_instance_sizes(index_path: &Path, class_index: &HashMap<u64, ClassDescriptor>) -> Result<()> {
    let tmp_path = index_path.with_extension("bin.tmp");

    let mut reader = BufReader::new(File::open(index_path).context("open object index for fixup")?);
    let mut writer = BufWriter::new(File::create(&tmp_path).context("create object index tmp")?);

    let mut buf: RawEntry = [0u8; ENTRY_SIZE];
    loop {
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e).context("read object index for fixup"),
        }

        let (_, class_id, _) = decode_entry(&buf);

        // InstanceDump: class_id is a real heap address (> 0x0B, no array flags).
        if class_id & OBJECT_ARRAY_FLAG == 0 && class_id > CLASS_ID_LONG_ARRAY {
            if let Some(desc) = class_index.get(&class_id) {
                // Overwrite bytes [16..20] with the correct instance_size.
                buf[16..20].copy_from_slice(&desc.instance_size.to_le_bytes());
            }
        }

        writer.write_all(&buf).context("write object index fixup")?;
    }

    writer.flush()?;
    drop(writer);
    drop(reader);

    std::fs::rename(&tmp_path, index_path).context("rename fixed object index")?;
    Ok(())
}

// ── Visitor ───────────────────────────────────────────────────────────────────

pub struct IndexVisitor {
    id_size: u32,
    /// string_id → UTF-8 string content.
    string_table: HashMap<u64, String>,
    /// class_object_id → class_name_string_id (from LoadClass records).
    name_id_map: HashMap<u64, u64>,
    /// class_object_id → ClassDescriptor (populated from ClassDump records).
    class_index: HashMap<u64, ClassDescriptor>,
    /// Accumulated GC root object IDs.
    roots: Vec<u64>,
    sorter: ExternalSorter,
    object_count: u64,
    output_dir: PathBuf,
}

impl IndexVisitor {
    pub fn new(id_size: u32, output_dir: &Path) -> Self {
        Self {
            id_size,
            string_table: HashMap::new(),
            name_id_map: HashMap::new(),
            class_index: HashMap::new(),
            roots: Vec::new(),
            sorter: ExternalSorter::new(output_dir.to_path_buf()),
            object_count: 0,
            output_dir: output_dir.to_path_buf(),
        }
    }

    fn primitive_array_shallow_size(&self, element_type: FieldType, num_elements: u32) -> u32 {
        // JVM object header (16) + array length field (4) + element data.
        16 + 4 + num_elements * element_type.byte_size(self.id_size)
    }

    fn object_array_shallow_size(&self, num_elements: u32) -> u32 {
        16 + 4 + num_elements * self.id_size
    }

    /// Finalise: resolve class names, write sorted index and roots to disk.
    pub fn into_output(mut self) -> Result<Pass1Output> {
        // Resolve class names now that all Utf8String + LoadClass records are seen.
        for (class_id, desc) in &mut self.class_index {
            if let Some(&name_sid) = self.name_id_map.get(class_id) {
                if let Some(raw) = self.string_table.get(&name_sid) {
                    // JVM internal names use '/' as separator; convert to '.'.
                    desc.name = raw.replace('/', ".");
                }
            }
        }

        // Write sorted object index.
        let index_path = self.output_dir.join("object_index.bin");
        let object_count = self.sorter.finish(&index_path)?;

        // Fix up shallow_size for InstanceDump entries.
        //
        // During streaming, InstanceDump provides only `data_size` (raw field bytes,
        // no JVM object header). The authoritative size is ClassDump.instance_size,
        // which includes the JVM object header (~16 bytes). We now have the full
        // class_index in memory, so patch every InstanceDump entry in place.
        fixup_instance_sizes(&index_path, &self.class_index)?;

        // Write roots.
        let roots_path = self.output_dir.join("roots.bin");
        let mut rw = BufWriter::new(File::create(&roots_path).context("create roots file")?);
        // De-duplicate: roots can be reported multiple times via different root types.
        self.roots.sort_unstable();
        self.roots.dedup();
        for &id in &self.roots {
            rw.write_all(&id.to_le_bytes())?;
        }
        rw.flush()?;

        // Write class names so later runs can skip re-parsing the HPROF.
        let class_names_path = self.output_dir.join("class_names.bin");
        write_class_names(&self.class_index, &class_names_path)
            .context("write class_names.bin")?;

        Ok(Pass1Output {
            class_index: self.class_index,
            roots: self.roots,
            object_count,
            object_index_path: index_path,
        })
    }

    fn handle_gc_record(&mut self, gc: GcRecord) {
        match gc {
            // ── GC roots ─────────────────────────────────────────────────────
            GcRecord::RootUnknown      { object_id }
            | GcRecord::RootJniGlobal  { object_id, .. }
            | GcRecord::RootJniLocal   { object_id, .. }
            | GcRecord::RootJavaFrame  { object_id, .. }
            | GcRecord::RootNativeStack{ object_id, .. }
            | GcRecord::RootStickyClass{ object_id }
            | GcRecord::RootThreadBlock{ object_id, .. }
            | GcRecord::RootMonitorUsed{ object_id }
            | GcRecord::RootThreadObject{object_id, .. } => {
                self.roots.push(object_id);
            }

            // ── Class definitions ─────────────────────────────────────────────
            GcRecord::ClassDump(fields) => {
                // Compute approximate shallow size of the class object itself:
                // JVM object header (16) + static field data.
                let static_bytes: u32 = fields
                    .static_fields
                    .iter()
                    .map(|(fi, _)| fi.field_type.byte_size(self.id_size))
                    .sum();
                let shallow = 16 + static_bytes;
                // Index the class object so it participates in reachability /
                // dominator computation.
                let entry = encode_entry(fields.class_object_id, CLASS_ID_JAVA_CLASS, shallow);
                self.sorter.push(entry).expect("object index write failed");
                self.object_count += 1;

                self.class_index.insert(
                    fields.class_object_id,
                    ClassDescriptor {
                        // Name resolved in into_output() once all Utf8String records are seen.
                        name: String::new(),
                        super_id: fields.super_class_object_id,
                        instance_size: fields.instance_size,
                        instance_fields: fields.instance_fields,
                    },
                );
            }

            // ── Object instances ─────────────────────────────────────────────
            GcRecord::InstanceDump { object_id, class_id, data_size, .. } => {
                // TODO: resolve shallow_size from class_index.instance_size after
                // all ClassDump records are seen. For now, data_size is the raw
                // field-data byte count — close but excludes the JVM object header.
                let entry = encode_entry(object_id, class_id, data_size);
                self.sorter.push(entry).expect("object index write failed");
                self.object_count += 1;
            }

            // ── Arrays ───────────────────────────────────────────────────────
            GcRecord::ObjectArrayDump { object_id, num_elements, element_class_id, .. } => {
                let shallow = self.object_array_shallow_size(num_elements);
                // Encode element_class_id with OBJECT_ARRAY_FLAG so the query layer
                // can distinguish "SomeClass[]" from plain "SomeClass" instances.
                let entry = encode_entry(object_id, element_class_id | OBJECT_ARRAY_FLAG, shallow);
                self.sorter.push(entry).expect("object index write failed");
                self.object_count += 1;
            }

            GcRecord::PrimitiveArrayDump { object_id, num_elements, element_type } => {
                let shallow = self.primitive_array_shallow_size(element_type, num_elements);
                // Use the FieldType discriminant as a synthetic class_id (values 4..11)
                // so the query layer can display the correct array type name.
                let entry = encode_entry(object_id, element_type as u64, shallow);
                self.sorter.push(entry).expect("object index write failed");
                self.object_count += 1;
            }
        }
    }
}

impl RecordVisitor for IndexVisitor {
    fn on_record(&mut self, record: Record) {
        match record {
            Record::Utf8String { id, str } => {
                self.string_table.insert(id, str.into_string());
            }
            Record::LoadClass(lc) => {
                self.name_id_map.insert(lc.class_object_id, lc.class_name_id);
            }
            Record::GcSegment(gc) => {
                self.handle_gc_record(gc);
            }
            // HeapDumpStart, HeapDumpEnd, Ignored — nothing to do.
            _ => {}
        }
    }
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
    for (&class_id, desc) in class_index {
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
        map.insert(class_id, ClassDescriptor {
            name,
            super_id,
            instance_size: 0,
            instance_fields: Vec::new(),
        });
    }
    Ok(map)
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
    let mut visitor = IndexVisitor::new(header.id_size, output_dir);
    process(path, &mut visitor).context("pass 1 streaming")?;
    visitor.into_output()
}
