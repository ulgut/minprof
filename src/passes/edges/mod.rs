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
//!
//! Reverse edges (`reverse_edges.bin`) are built by reading the sorted forward
//! file after the forward merge completes, swapping (from, to) → (to, from),
//! and sorting again. This avoids per-edge channel overhead (~200-500 ns/edge
//! for `sync_channel` × 1B edges = 200-500 s of pure overhead).

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::parser::gc_record::FieldType;
use crate::parser::primitive_parsers::read_id_be;
use crate::parser::record_stream_parser::{process_with_extractor, read_header};
use crate::passes::index::{ClassDescriptor, Pass1Output};
use crate::passes::sort::RecordSorter;

// ── On-disk edge format ──────────────────────────────────────────────────────
//
//  [0..8]  from_id: u64  (little-endian)
//  [8..16] to_id:   u64  (little-endian)

pub const EDGE_SIZE: usize = 16;
pub type RawEdge = [u8; EDGE_SIZE];

use crate::passes::IO_BUF_SIZE;

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

// ── Edge sort key ────────────────────────────────────────────────────────────

fn key_edge(e: &RawEdge) -> (u64, u64) {
    (edge_from(e), edge_to(e))
}

// ── Pass 2 output ─────────────────────────────────────────────────────────────

pub struct Pass2Output {
    /// Sorted forward edge file: (from_id, to_id) pairs sorted by from_id.
    pub edges_path: PathBuf,
    pub edge_count: u64,
}

// ── Inline edge extractor ─────────────────────────────────────────────────────

/// Stateful HPROF parser that extracts reference edges directly from the raw
/// byte stream with zero per-object allocation. Runs in the extractor thread.
struct EdgeStreamExtractor {
    id_size: usize,
    /// Precomputed per-class flat list of byte offsets (within the InstanceDump
    /// raw-data block) at which an Object-typed field lives.
    field_offsets: HashMap<u64, Vec<u32>>,
    heap_dump_remaining: usize,
}

impl EdgeStreamExtractor {
    fn new(id_size: u32, class_index: &HashMap<u64, ClassDescriptor>) -> Self {
        let is = id_size as usize;
        let mut field_offsets: HashMap<u64, Vec<u32>> = HashMap::with_capacity(class_index.len());

        for &class_id in class_index.keys() {
            let mut offsets: Vec<u32> = Vec::new();
            let mut cursor: u32 = 0;
            let mut cur_id = class_id;

            while cur_id != 0 {
                let Some(desc) = class_index.get(&cur_id) else {
                    break;
                };
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
    fn extract(&mut self, buf: &[u8], out: &mut Vec<RawEdge>) -> usize {
        let mut pos = 0;

        loop {
            let rem = &buf[pos..];
            if rem.is_empty() {
                break;
            }

            if self.heap_dump_remaining == 0 {
                if rem.len() < 9 {
                    break;
                }
                let tag = rem[0];
                let length = u32::from_be_bytes(rem[5..9].try_into().unwrap()) as usize;
                match tag {
                    0x0C | 0x1C => {
                        self.heap_dump_remaining = length;
                        pos += 9;
                    }
                    0x2C => {
                        pos += 9;
                    }
                    _ => {
                        let total = 9 + length;
                        if rem.len() < total {
                            break;
                        }
                        pos += total;
                    }
                }
            } else {
                let n = self.extract_gc(rem, out);
                if n == 0 {
                    break;
                }
                self.heap_dump_remaining = self.heap_dump_remaining.saturating_sub(n);
                pos += n;
            }
        }

        pos
    }

    fn extract_gc(&self, buf: &[u8], out: &mut Vec<RawEdge>) -> usize {
        if buf.is_empty() {
            return 0;
        }
        let is = self.id_size;
        let tag = buf[0];
        let data = &buf[1..];

        match tag {
            0x21 => {
                // TAG_GC_INSTANCE_DUMP
                let hdr = 2 * is + 8;
                if data.len() < hdr {
                    return 0;
                }
                let object_id = read_id_be(is, data);
                let class_id = read_id_be(is, &data[is + 4..]);
                let data_size =
                    u32::from_be_bytes(data[2 * is + 4..2 * is + 8].try_into().unwrap()) as usize;
                let total = 1 + hdr + data_size;
                if buf.len() < total {
                    return 0;
                }
                let raw = &data[hdr..hdr + data_size];
                self.extract_instance_edges(object_id, class_id, raw, out);
                total
            }
            0x22 => {
                // TAG_GC_OBJ_ARRAY_DUMP
                let hdr = 2 * is + 8;
                if data.len() < hdr {
                    return 0;
                }
                let object_id = read_id_be(is, data);
                let num_elements =
                    u32::from_be_bytes(data[is + 4..is + 8].try_into().unwrap()) as usize;
                let payload = num_elements * is;
                let total = 1 + hdr + payload;
                if buf.len() < total {
                    return 0;
                }
                let elem_data = &data[hdr..hdr + payload];
                for chunk in elem_data.chunks_exact(is) {
                    let to = read_id_be(is, chunk);
                    if to != 0 {
                        out.push(encode_edge(object_id, to));
                    }
                }
                total
            }
            0x23 => {
                // TAG_GC_PRIM_ARRAY_DUMP
                let hdr = is + 9;
                if data.len() < hdr {
                    return 0;
                }
                let num_elements =
                    u32::from_be_bytes(data[is + 4..is + 8].try_into().unwrap()) as usize;
                let elem_type = data[is + 8];
                let total = 1 + hdr + num_elements * field_type_size(elem_type, is);
                if buf.len() < total {
                    return 0;
                }
                total
            }
            0x20 => {
                // TAG_GC_CLASS_DUMP
                match class_dump_size_and_edges(is, data, out) {
                    Some(body_size) => 1 + body_size,
                    None => 0,
                }
            }
            0xFF => {
                if data.len() < is {
                    return 0;
                }
                1 + is
            }
            0x01 => {
                if data.len() < 2 * is {
                    return 0;
                }
                1 + 2 * is
            }
            0x02 | 0x03 => {
                if data.len() < is + 8 {
                    return 0;
                }
                1 + is + 8
            }
            0x04 | 0x06 => {
                if data.len() < is + 4 {
                    return 0;
                }
                1 + is + 4
            }
            0x05 | 0x07 => {
                if data.len() < is {
                    return 0;
                }
                1 + is
            }
            0x08 => {
                if data.len() < is + 8 {
                    return 0;
                }
                1 + is + 8
            }
            x => panic!("unknown GC sub-record tag: 0x{x:02X}"),
        }
    }

    #[inline]
    fn extract_instance_edges(&self, from: u64, class_id: u64, raw: &[u8], out: &mut Vec<RawEdge>) {
        let is = self.id_size;
        let Some(offsets) = self.field_offsets.get(&class_id) else {
            return;
        };
        for &off in offsets {
            let off = off as usize;
            if off + is > raw.len() {
                break;
            }
            let to = read_id_be(is, &raw[off..]);
            if to != 0 {
                out.push(encode_edge(from, to));
            }
        }
    }
}

// ── Inline parser helpers ─────────────────────────────────────────────────────

fn field_type_size(ty: u8, is: usize) -> usize {
    match ty {
        2 => is,
        4 | 8 => 1,
        5 | 9 => 2,
        6 | 10 => 4,
        7 | 11 => 8,
        _ => panic!("unknown field type byte: {ty}"),
    }
}

fn class_dump_size_and_edges(is: usize, buf: &[u8], out: &mut Vec<RawEdge>) -> Option<usize> {
    let fixed = 7 * is + 8;
    if buf.len() < fixed + 2 {
        return None;
    }

    let class_object_id = read_id_be(is, buf);
    let mut pos = fixed;

    // Constant pool
    let cp_count = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    for _ in 0..cp_count {
        if buf.len() < pos + 3 {
            return None;
        }
        pos += 2;
        let ty = buf[pos];
        pos += 1;
        let sz = field_type_size(ty, is);
        if buf.len() < pos + sz {
            return None;
        }
        pos += sz;
    }

    // Static fields
    if buf.len() < pos + 2 {
        return None;
    }
    let static_count = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    for _ in 0..static_count {
        if buf.len() < pos + is + 1 {
            return None;
        }
        let ty = buf[pos + is];
        let sz = field_type_size(ty, is);
        if buf.len() < pos + is + 1 + sz {
            return None;
        }
        if ty == 2 {
            let to = read_id_be(is, &buf[pos + is + 1..]);
            if to != 0 {
                out.push(encode_edge(class_object_id, to));
            }
        }
        pos += is + 1 + sz;
    }

    // Instance fields — skip (name_id + type, no value)
    if buf.len() < pos + 2 {
        return None;
    }
    let instance_count = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    let descriptors_size = instance_count * (is + 1);
    if buf.len() < pos + descriptors_size {
        return None;
    }
    pos += descriptors_size;

    Some(pos)
}

// ── Build reverse edges from sorted forward file ─────────────────────────────

/// Read `edges.bin` (sorted by from_id), swap each (from, to) → (to, from),
/// and sort into `reverse_edges.bin`.
///
/// Called on-demand (only when `--path` is used) so that normal runs skip
/// the ~40 s reverse-edge sort entirely.
pub fn build_reverse_edges(
    edges_path: &Path,
    reverse_path: &Path,
    output_dir: &Path,
) -> Result<()> {
    let mut rev_sorter =
        RecordSorter::<EDGE_SIZE>::new(output_dir.to_path_buf(), "rev_edge", key_edge).dedup();

    let mut reader = BufReader::with_capacity(
        IO_BUF_SIZE,
        File::open(edges_path).context("open edges.bin for reverse build")?,
    );
    loop {
        let consumed = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let records = buf.len() / EDGE_SIZE;
            for chunk in buf[..records * EDGE_SIZE].chunks_exact(EDGE_SIZE) {
                let from = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
                let to = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
                rev_sorter.push(encode_edge(to, from))?;
            }
            records * EDGE_SIZE
        };
        reader.consume(consumed);
    }

    rev_sorter.finish(reverse_path)?;
    Ok(())
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(path: &Path, pass1: &Pass1Output, output_dir: &Path) -> Result<Pass2Output> {
    let (header, _) = read_header(path)?;
    let id_size = header.id_size;

    let mut sorter =
        RecordSorter::<EDGE_SIZE>::new(output_dir.to_path_buf(), "edge", key_edge).dedup();

    {
        let mut extractor = EdgeStreamExtractor::new(id_size, &pass1.class_index);
        process_with_extractor(
            path,
            move |buf: &[u8], edges: &mut Vec<RawEdge>| -> usize { extractor.extract(buf, edges) },
            &mut |batch: &mut Vec<RawEdge>| {
                for edge in batch.iter() {
                    sorter.push(*edge).expect("edge sort write failed");
                }
            },
        )
        .context("pass 2 streaming")?;
    }

    let edges_path = output_dir.join("edges.bin");

    // Forward merge — sort buffer is freed inside finish().
    let edge_count = sorter.finish(&edges_path)?;

    Ok(Pass2Output {
        edges_path,
        edge_count,
    })
}
