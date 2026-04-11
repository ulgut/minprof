//! Disk-backed index structures shared across the query layer.
//!
//! Re-exports the encoding constants defined in `passes::index` so callers
//! only need to import from here.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};

use crate::passes::index::{ClassDescriptor, ENTRY_SIZE};
use std::collections::HashMap;

// ── Re-export encoding constants ─────────────────────────────────────────────

pub use crate::passes::index::{
    CLASS_ID_JAVA_CLASS,
    CLASS_ID_BOOL_ARRAY, CLASS_ID_CHAR_ARRAY,
    CLASS_ID_FLOAT_ARRAY, CLASS_ID_DOUBLE_ARRAY,
    CLASS_ID_BYTE_ARRAY, CLASS_ID_SHORT_ARRAY,
    CLASS_ID_INT_ARRAY, CLASS_ID_LONG_ARRAY,
    OBJECT_ARRAY_FLAG,
};

// ── Class name resolution ────────────────────────────────────────────────────

/// Convert a JVM internal class name (as stored in class_index after `/`→`.`
/// replacement) to a human-readable display name.
///
/// | Stored name             | Display name              |
/// |-------------------------|---------------------------|
/// | `java.lang.Object`      | `java.lang.Object`        |
/// | `[Ljava.lang.Object;`   | `java.lang.Object[]`      |
/// | `[[I`                   | `int[][]`                 |
/// | `[B`                    | `byte[]`                  |
fn normalize_class_name(raw: &str) -> String {
    let dims = raw.bytes().take_while(|&b| b == b'[').count();
    if dims == 0 {
        return raw.to_string();
    }
    let inner = &raw[dims..];
    let base = if inner.starts_with('L') && inner.ends_with(';') {
        // Reference type: `Ljava.lang.Object;` → `java.lang.Object`
        inner[1..inner.len() - 1].to_string()
    } else {
        match inner {
            "I" => "int",
            "C" => "char",
            "B" => "byte",
            "Z" => "boolean",
            "S" => "short",
            "F" => "float",
            "D" => "double",
            "J" => "long",
            other => other,
        }.to_string()
    };
    format!("{}{}", base, "[]".repeat(dims))
}

/// Resolve a `class_id` (as stored in `object_index.bin`) to a display name.
///
/// | class_id value                  | Meaning                              |
/// |---------------------------------|--------------------------------------|
/// | `CLASS_ID_JAVA_CLASS` (0x01)    | `java.lang.Class`                   |
/// | `CLASS_ID_*_ARRAY` (0x04..0x0B) | Primitive array type (int[], …)      |
/// | `class_id \| OBJECT_ARRAY_FLAG` | Reference array (element name + "[]")|
/// | anything else                   | Look up in `class_index`             |
pub fn class_name(class_id: u64, class_index: &HashMap<u64, ClassDescriptor>) -> String {
    let is_obj_arr = class_id & OBJECT_ARRAY_FLAG != 0;
    let cid = class_id & !OBJECT_ARRAY_FLAG;

    if is_obj_arr {
        // HPROF ObjectArrayDump stores the class object of the ARRAY TYPE itself
        // (e.g., `[Ljava.lang.Object;` for `Object[]`, `[[Ljava.lang.String;` for
        // `String[][]`).  Normalising that descriptor gives the correct display name
        // without needing to append an extra "[]".
        //
        // Fallback: if the stored class_id resolves to a plain (non-array) class
        // name, the JVM stored the element type instead — append "[]" in that case.
        let raw = class_index
            .get(&cid)
            .map(|d| d.name.as_str())
            .unwrap_or("");
        if raw.starts_with('[') {
            return normalize_class_name(raw);
        } else if raw.is_empty() {
            return format!("<unknown-array@0x{cid:x}>");
        } else {
            return format!("{}[]", normalize_class_name(raw));
        }
    }

    match cid {
        CLASS_ID_JAVA_CLASS   => "java.lang.Class".to_string(),
        CLASS_ID_BOOL_ARRAY   => "boolean[]".to_string(),
        CLASS_ID_CHAR_ARRAY   => "char[]".to_string(),
        CLASS_ID_FLOAT_ARRAY  => "float[]".to_string(),
        CLASS_ID_DOUBLE_ARRAY => "double[]".to_string(),
        CLASS_ID_BYTE_ARRAY   => "byte[]".to_string(),
        CLASS_ID_SHORT_ARRAY  => "short[]".to_string(),
        CLASS_ID_INT_ARRAY    => "int[]".to_string(),
        CLASS_ID_LONG_ARRAY   => "long[]".to_string(),
        _ => resolve_instance_name(cid, class_index),
    }
}

fn resolve_instance_name(class_id: u64, class_index: &HashMap<u64, ClassDescriptor>) -> String {
    class_index
        .get(&class_id)
        .map(|d| normalize_class_name(&d.name))
        .unwrap_or_else(|| format!("<unknown@0x{class_id:x}>"))
}

// ── ObjectIndex ──────────────────────────────────────────────────────────────

/// Streaming reader for `object_index.bin`.
///
/// Entries are sorted by `object_id` and encoded as:
/// `[object_id: u64 LE][class_id: u64 LE][shallow_size: u32 LE]`
///
/// `class_id` uses the synthetic encoding described in `class_name()`.
pub struct ObjectIndex {
    path: std::path::PathBuf,
    /// Total number of entries in the file.
    pub entry_count: usize,
}

impl ObjectIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let len = std::fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len() as usize;
        Ok(Self {
            path: path.to_path_buf(),
            entry_count: len / ENTRY_SIZE,
        })
    }

    /// Return a streaming iterator over `(object_id, class_id, shallow_size)` tuples.
    /// Entries arrive in sorted `object_id` order.
    pub fn iter(&self) -> Result<ObjectIndexIter> {
        let reader = BufReader::new(
            File::open(&self.path).context("open object index")?,
        );
        Ok(ObjectIndexIter { reader, buf: [0u8; ENTRY_SIZE] })
    }
}

pub struct ObjectIndexIter {
    reader: BufReader<File>,
    buf: [u8; ENTRY_SIZE],
}

impl Iterator for ObjectIndexIter {
    type Item = (u64, u64, u32); // (object_id, class_id, shallow_size)

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.read_exact(&mut self.buf).ok()?;
        let oid = u64::from_le_bytes(self.buf[0..8].try_into().unwrap());
        let cid = u64::from_le_bytes(self.buf[8..16].try_into().unwrap());
        let sz  = u32::from_le_bytes(self.buf[16..20].try_into().unwrap());
        Some((oid, cid, sz))
    }
}

// ── RetainedIndex ────────────────────────────────────────────────────────────

/// In-memory retained-size array.
///
/// `retained[i]` = retained heap bytes of the object at node index `i`
/// (i.e., position `i` in `object_index.bin`).
pub struct RetainedIndex {
    data: Vec<u64>,
}

impl RetainedIndex {
    pub fn load(path: &Path) -> Result<Self> {
        let len = std::fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len() as usize;
        let count = len / 8;
        let mut data = Vec::with_capacity(count);
        let mut reader = BufReader::new(File::open(path).context("open retained.bin")?);
        let mut buf = [0u8; 8];
        while reader.read_exact(&mut buf).is_ok() {
            data.push(u64::from_le_bytes(buf));
        }
        Ok(Self { data })
    }

    #[inline]
    pub fn get(&self, node_idx: usize) -> u64 {
        self.data[node_idx]
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
