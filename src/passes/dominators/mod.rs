//! Pass 3 — dominator tree computation.
//!
//! Uses the Cooper/Harvey/Kennedy (CHK) iterative algorithm, which is simpler
//! than Lengauer-Tarjan while converging quickly in practice for heap graphs.
//!
//! # Node numbering
//!
//! Actual objects are assigned node indices 0..N-1 corresponding to their
//! position in the sorted `object_index.bin`. A virtual root gets index N;
//! it has edges to all GC roots and dominates every node.
//!
//! # RPO indexing
//!
//! The CHK algorithm requires nodes to be processed in reverse post-order
//! (RPO). We compute RPO via iterative DFS from the virtual root.
//! Lower RPO number = higher in the dominator tree (virtual root = RPO 0).
//!
//! `idom[j]` is indexed by RPO number and stores the RPO number of `j`'s
//! immediate dominator. Unreachable nodes get `UNDEFINED`.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::passes::edges::{EDGE_SIZE, Pass2Output};
use crate::passes::index::Pass1Output;
use crate::passes::sort::RecordSorter;
use crate::passes::IO_BUF_SIZE;

// ── Constants ─────────────────────────────────────────────────────────────────

const UNDEFINED: u32 = u32::MAX;

// Intermediate record sizes:
//   PARTIAL_SIZE: (key_id: u64, idx: u32)  — first sort pass result
//   INDEXED_SIZE: (idx_a: u32, idx_b: u32) — second sort pass result / CSR input
const PARTIAL_SIZE: usize = 12;
const INDEXED_SIZE: usize = 8;

// ── Output ────────────────────────────────────────────────────────────────────

pub struct Pass3Output {
    /// Path to idom file: `u32` array indexed by RPO number, each value is
    /// the RPO number of the immediate dominator. `UNDEFINED` for unreachable.
    pub idom_path: PathBuf,
    /// RPO number → node index (position in object_index.bin).
    pub rpo_to_node: Vec<u32>,
    /// N: number of actual object nodes.
    pub node_count: u32,
}

// ── CSR (Compressed Sparse Row) adjacency list ────────────────────────────────

struct Csr {
    /// offsets[i]..offsets[i+1] is the range of `neighbors` for node i.
    /// u64 because total edge count can exceed u32::MAX on large heap dumps.
    offsets: Vec<u64>,
    neighbors: Vec<u32>,
}

impl Csr {
    fn neighbors(&self, node: u32) -> &[u32] {
        let start = self.offsets[node as usize] as usize;
        let end = self.offsets[node as usize + 1] as usize;
        &self.neighbors[start..end]
    }
}

// ── Step 1: load object IDs ───────────────────────────────────────────────────

use crate::passes::index::load_object_ids;

/// Binary search: object_id → node index (position in sorted `ids`).
/// Returns `None` if the object_id is not in the index (malformed dump).
fn lookup_node(ids: &[u64], object_id: u64) -> Option<u32> {
    ids.binary_search(&object_id).ok().map(|i| i as u32)
}

// ── Step 2: load GC root node indices ────────────────────────────────────────

fn load_root_nodes(roots: &[u64], ids: &[u64]) -> Vec<u32> {
    let mut root_nodes: Vec<u32> = roots
        .iter()
        .filter_map(|&id| lookup_node(ids, id))
        .collect();
    root_nodes.sort_unstable();
    root_nodes.dedup();
    root_nodes
}

// ── Step 3: build forward and reverse CSR from edge files ────────────────────
//
// Strategy: eliminate all binary searches by resolving object IDs to node
// indices via merge-scan (possible because each edge file is sorted by its
// first field).  Since a single scan can only handle one sorted field at a
// time, we use a two-sort pipeline for each CSR:
//
//   Forward CSR  (edges.bin sorted by from_id):
//     A. Scan edges.bin, merge-scan from_id → from_idx,
//        write (to_id: u64, from_idx: u32) → sort by to_id
//     B. Scan sorted partial, merge-scan to_id → to_idx,
//        write (from_idx: u32, to_idx: u32) → sort by from_idx
//     C. Build CSR from sorted (from_idx, to_idx) — pure streaming, no lookup
//
//   Reverse CSR  (reverse_edges.bin sorted by to_id):
//     A. Scan reverse_edges.bin, merge-scan to_id → to_idx,
//        write (from_id: u64, to_idx: u32) → sort by from_id
//     B. Scan sorted partial, merge-scan from_id → from_idx,
//        write (to_idx: u32, from_idx: u32) → sort by to_idx
//     C. Build CSR from sorted (to_idx, from_idx), then append vroot edges
//
// This turns O(E · log N) random DRAM latency into O(E) streaming I/O
// plus two external sorts per CSR.

fn build_csrs(
    edges_path: &Path,
    rev_edges_path: &Path,
    ids: Vec<u64>,
    root_nodes: &[u32],
    output_dir: &Path,
) -> Result<(Csr, Csr)> {
    let n = ids.len();
    let vroot = n as u32;

    // Produce the indexed (sorted by node-index) files for both CSRs.
    // These run sequentially to bound peak RAM to ids[] + one sort buffer.
    let fwd_indexed = resolve_forward_indexed(edges_path, &ids, output_dir)?;
    let rev_indexed = resolve_reverse_indexed(rev_edges_path, &ids, root_nodes, vroot, output_dir)?;

    // ids[] no longer needed — free 4 GB before allocating the CSRs.
    drop(ids);

    let forward = build_csr_from_indexed(&fwd_indexed, n, /*extra_nodes=*/ 0)?;
    let _ = std::fs::remove_file(&fwd_indexed);

    let reverse = build_csr_from_indexed(&rev_indexed, n + 1, /*extra_nodes=*/ 0)?;
    let _ = std::fs::remove_file(&rev_indexed);

    Ok((forward, reverse))
}

// ── 3a: produce fwd_indexed.bin sorted by (from_idx, to_idx) ─────────────────

fn key_partial(rec: &[u8; PARTIAL_SIZE]) -> (u64, u64) {
    let key = u64::from_le_bytes(rec[0..8].try_into().unwrap());
    let idx = u32::from_le_bytes(rec[8..12].try_into().unwrap()) as u64;
    (key, idx)
}

fn key_indexed(rec: &[u8; INDEXED_SIZE]) -> (u64, u64) {
    let a = u32::from_le_bytes(rec[0..4].try_into().unwrap()) as u64;
    let b = u32::from_le_bytes(rec[4..8].try_into().unwrap()) as u64;
    (a, b)
}

/// edges.bin (sorted by from_id) → fwd_indexed.bin (sorted by from_idx, to_idx).
///
/// Two sort passes:
///   1. Resolve from_id via merge-scan → write (to_id, from_idx), sort by to_id.
///   2. Resolve to_id via merge-scan → write (from_idx, to_idx), sort by from_idx.
fn resolve_forward_indexed(edges_path: &Path, ids: &[u64], output_dir: &Path) -> Result<PathBuf> {
    // ── Pass A: merge-scan from_id → from_idx; collect (to_id, from_idx) ─────
    let partial_path = output_dir.join("fwd_partial_sorted.bin");
    {
        let mut sorter = RecordSorter::<PARTIAL_SIZE>::new(
            output_dir.to_path_buf(),
            "fwd_partial",
            key_partial,
        );
        let mut reader =
            BufReader::with_capacity(IO_BUF_SIZE, File::open(edges_path).context("open edges")?);
        let mut buf = [0u8; EDGE_SIZE];
        let mut scan = 0usize;
        while reader.read_exact(&mut buf).is_ok() {
            let from_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let to_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            while scan < ids.len() && ids[scan] < from_id {
                scan += 1;
            }
            if scan >= ids.len() || ids[scan] != from_id {
                continue;
            }
            let from_idx = scan as u32;
            let mut rec = [0u8; PARTIAL_SIZE];
            rec[0..8].copy_from_slice(&to_id.to_le_bytes());
            rec[8..12].copy_from_slice(&from_idx.to_le_bytes());
            sorter.push(rec)?;
        }
        sorter.finish(&partial_path)?;
    }
    eprintln!("  [fwd] partial resolve done");

    // ── Pass B: merge-scan to_id → to_idx; collect (from_idx, to_idx) ───────
    let indexed_path = output_dir.join("fwd_indexed_sorted.bin");
    {
        let mut sorter = RecordSorter::<INDEXED_SIZE>::new(
            output_dir.to_path_buf(),
            "fwd_indexed",
            key_indexed,
        );
        let mut reader = BufReader::with_capacity(
            IO_BUF_SIZE,
            File::open(&partial_path).context("open fwd partial")?,
        );
        let mut buf = [0u8; PARTIAL_SIZE];
        let mut scan = 0usize;
        while reader.read_exact(&mut buf).is_ok() {
            let to_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let from_idx = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            while scan < ids.len() && ids[scan] < to_id {
                scan += 1;
            }
            if scan >= ids.len() || ids[scan] != to_id {
                continue;
            }
            let to_idx = scan as u32;
            let mut rec = [0u8; INDEXED_SIZE];
            rec[0..4].copy_from_slice(&from_idx.to_le_bytes());
            rec[4..8].copy_from_slice(&to_idx.to_le_bytes());
            sorter.push(rec)?;
        }
        sorter.finish(&indexed_path)?;
    }
    let _ = std::fs::remove_file(&partial_path);
    eprintln!("  [fwd] indexed resolve done");

    Ok(indexed_path)
}

// ── 3b: produce rev_indexed.bin sorted by (to_idx, from_idx) ─────────────────

/// reverse_edges.bin (sorted by to_id) → rev_indexed.bin (sorted by to_idx, from_idx).
/// Also appends virtual-root edges: for each GC root r, predecessor = vroot.
///
/// Two sort passes:
///   1. Resolve to_id via merge-scan → write (from_id, to_idx), sort by from_id.
///   2. Resolve from_id via merge-scan → write (to_idx, from_idx), sort by to_idx.
///   3. Append virtual-root entries (r, vroot) for each root_node r.
fn resolve_reverse_indexed(
    rev_edges_path: &Path,
    ids: &[u64],
    root_nodes: &[u32],
    vroot: u32,
    output_dir: &Path,
) -> Result<PathBuf> {
    // ── Pass A: merge-scan to_id → to_idx; collect (from_id, to_idx) ────────
    let partial_path = output_dir.join("rev_partial_sorted.bin");
    {
        let mut sorter = RecordSorter::<PARTIAL_SIZE>::new(
            output_dir.to_path_buf(),
            "rev_partial",
            key_partial,
        );
        let mut reader = BufReader::with_capacity(
            IO_BUF_SIZE,
            File::open(rev_edges_path).context("open rev edges")?,
        );
        let mut buf = [0u8; EDGE_SIZE];
        let mut scan = 0usize;
        while reader.read_exact(&mut buf).is_ok() {
            // reverse_edges.bin layout: (to_id, from_id)
            let to_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let from_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            while scan < ids.len() && ids[scan] < to_id {
                scan += 1;
            }
            if scan >= ids.len() || ids[scan] != to_id {
                continue;
            }
            let to_idx = scan as u32;
            let mut rec = [0u8; PARTIAL_SIZE];
            rec[0..8].copy_from_slice(&from_id.to_le_bytes());
            rec[8..12].copy_from_slice(&to_idx.to_le_bytes());
            sorter.push(rec)?;
        }
        sorter.finish(&partial_path)?;
    }
    eprintln!("  [rev] partial resolve done");

    // ── Pass B: merge-scan from_id → from_idx; collect (to_idx, from_idx) ───
    let indexed_path = output_dir.join("rev_indexed_sorted.bin");
    {
        let mut sorter = RecordSorter::<INDEXED_SIZE>::new(
            output_dir.to_path_buf(),
            "rev_indexed",
            key_indexed,
        );
        let mut reader = BufReader::with_capacity(
            IO_BUF_SIZE,
            File::open(&partial_path).context("open rev partial")?,
        );
        let mut buf = [0u8; PARTIAL_SIZE];
        let mut scan = 0usize;
        while reader.read_exact(&mut buf).is_ok() {
            let from_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let to_idx = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            while scan < ids.len() && ids[scan] < from_id {
                scan += 1;
            }
            if scan >= ids.len() || ids[scan] != from_id {
                continue;
            }
            let from_idx = scan as u32;
            let mut rec = [0u8; INDEXED_SIZE];
            rec[0..4].copy_from_slice(&to_idx.to_le_bytes());
            rec[4..8].copy_from_slice(&from_idx.to_le_bytes());
            sorter.push(rec)?;
        }
        // Append virtual-root edges: for each GC root r, its predecessor is vroot.
        // These are (to_idx=r, from_idx=vroot) entries.
        for &r in root_nodes {
            let mut rec = [0u8; INDEXED_SIZE];
            rec[0..4].copy_from_slice(&r.to_le_bytes());
            rec[4..8].copy_from_slice(&vroot.to_le_bytes());
            sorter.push(rec)?;
        }
        sorter.finish(&indexed_path)?;
    }
    let _ = std::fs::remove_file(&partial_path);
    eprintln!("  [rev] indexed resolve done");

    Ok(indexed_path)
}

// ── 3c: build a CSR from a sorted (node_a: u32, node_b: u32) file ────────────

/// Build a CSR from a file of `(node_a: u32, node_b: u32)` pairs sorted by
/// `node_a`. `total_nodes` is the number of distinct source nodes (= size of
/// the `offsets` array minus one).
///
/// Because the input is sorted, the fill pass writes `neighbors` purely
/// sequentially — no cursor array, no random writes.
fn build_csr_from_indexed(indexed_path: &Path, total_nodes: usize, _extra_nodes: usize) -> Result<Csr> {
    let mut offsets = vec![0u64; total_nodes + 1];

    // Count pass: tally out-degree per source node.
    {
        let mut reader = BufReader::with_capacity(
            IO_BUF_SIZE,
            File::open(indexed_path).context("open indexed file (count)")?,
        );
        let mut buf = [0u8; INDEXED_SIZE];
        while reader.read_exact(&mut buf).is_ok() {
            let node_a = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if node_a < total_nodes {
                offsets[node_a + 1] += 1;
            }
        }
    }
    for i in 1..=total_nodes {
        offsets[i] += offsets[i - 1];
    }

    let edge_count = offsets[total_nodes] as usize;
    let mut neighbors = vec![0u32; edge_count];

    // Fill pass: data is sorted by node_a → writes to neighbors are sequential.
    {
        let mut reader = BufReader::with_capacity(
            IO_BUF_SIZE,
            File::open(indexed_path).context("open indexed file (fill)")?,
        );
        let mut buf = [0u8; INDEXED_SIZE];
        let mut write_pos = 0usize;
        while reader.read_exact(&mut buf).is_ok() {
            let node_a = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if node_a < total_nodes {
                let node_b = u32::from_le_bytes(buf[4..8].try_into().unwrap());
                neighbors[write_pos] = node_b;
                write_pos += 1;
            }
        }
    }

    Ok(Csr { offsets, neighbors })
}

// ── Step 4: iterative DFS for RPO numbering ───────────────────────────────────

/// Compute RPO numbering via iterative DFS from the virtual root.
///
/// Returns `(node_to_rpo, rpo_to_node)`:
/// - `node_to_rpo[i]`  = RPO number of node i (`UNDEFINED` if unreachable)
/// - `rpo_to_node[j]`  = node index with RPO number j
fn compute_rpo(n: u32, vroot: u32, root_nodes: &[u32], forward: &Csr) -> (Vec<u32>, Vec<u32>) {
    let total = (n + 1) as usize; // actual nodes + virtual root
    let mut node_to_rpo = vec![UNDEFINED; total];

    // Packed-bit visited set: 8× smaller than Vec<bool> → 8× more nodes per
    // cache line → dramatically fewer DRAM misses for random successor checks.
    // 500 M nodes → 62.5 MB (vs 500 MB for Vec<bool>).
    let mut visited: Vec<u64> = vec![0u64; (total + 63) / 64];
    let mut post_order: Vec<u32> = Vec::with_capacity(total);

    // Stack stores (node, abs_cur, abs_end) where abs_cur/abs_end are absolute
    // indices into forward.neighbors (pre-computed on node push).  This avoids
    // re-reading offsets[node] on every edge visit — only one lookup per node.
    //
    // vroot's successors are root_nodes, not the forward CSR, so we keep a
    // separate cursor for it (abs_cur/abs_end encode a root_nodes index when
    // node == vroot, identified by the sentinel abs_end == u32::MAX).
    let mut stack: Vec<(u32, u32, u32)> = Vec::new();

    macro_rules! visit_set {
        ($i:expr) => { visited[$i / 64] |= 1u64 << ($i % 64) };
    }
    macro_rules! visit_get {
        ($i:expr) => { (visited[$i / 64] >> ($i % 64)) & 1 != 0 };
    }

    visit_set!(vroot as usize);
    // Sentinel: abs_end = u32::MAX means "use root_nodes[abs_cur]".
    stack.push((vroot, 0, u32::MAX));

    while let Some(&mut (node, ref mut abs_cur, abs_end)) = stack.last_mut() {
        let succ = if abs_end == u32::MAX {
            // vroot frame: iterate root_nodes
            let idx = *abs_cur as usize;
            if idx < root_nodes.len() {
                *abs_cur += 1;
                Some(root_nodes[idx])
            } else {
                None
            }
        } else {
            // Normal frame: iterate forward.neighbors[abs_cur..abs_end]
            if *abs_cur < abs_end {
                let s = forward.neighbors[*abs_cur as usize];
                *abs_cur += 1;
                Some(s)
            } else {
                None
            }
        };

        if let Some(s) = succ {
            if !visit_get!(s as usize) {
                visit_set!(s as usize);
                let start = forward.offsets[s as usize] as u32;
                let end = forward.offsets[s as usize + 1] as u32;
                stack.push((s, start, end));
            }
        } else {
            post_order.push(node);
            stack.pop();
        }
    }

    // RPO = reverse of post-order.
    let rpo_count = post_order.len();
    let mut rpo_to_node = vec![0u32; rpo_count];
    for (post_num, &node) in post_order.iter().enumerate() {
        let rpo = rpo_count - 1 - post_num;
        node_to_rpo[node as usize] = rpo as u32;
        rpo_to_node[rpo] = node;
    }

    (node_to_rpo, rpo_to_node)
}

// ── Step 5: CHK iterative dominator algorithm ─────────────────────────────────

/// Walk up the dominator tree (following idom links) until b1 and b2 meet.
/// Operands and return value are RPO numbers; lower = higher in tree.
fn intersect(mut b1: u32, mut b2: u32, idom: &[u32]) -> u32 {
    while b1 != b2 {
        while b1 > b2 {
            b1 = idom[b1 as usize];
        }
        while b2 > b1 {
            b2 = idom[b2 as usize];
        }
    }
    b1
}

/// Compute the immediate dominator for every node using the CHK algorithm.
///
/// Returns `idom` indexed by RPO number; each value is the RPO number of
/// the immediate dominator. Virtual root (RPO 0) maps to itself.
fn compute_dominators(rpo_to_node: &[u32], node_to_rpo: &[u32], reverse: &Csr) -> Vec<u32> {
    let rpo_count = rpo_to_node.len();
    let mut idom = vec![UNDEFINED; rpo_count];
    idom[0] = 0; // virtual root dominates itself

    let mut changed = true;
    let mut iterations = 0u32;

    while changed {
        changed = false;
        iterations += 1;

        // Process all nodes in RPO order, skipping the virtual root (RPO 0).
        for rpo_b in 1..rpo_count {
            let b = rpo_to_node[rpo_b]; // actual node index

            let mut new_idom: Option<u32> = None;

            for &pred_node in reverse.neighbors(b) {
                let pred_rpo = node_to_rpo[pred_node as usize];
                if pred_rpo == UNDEFINED {
                    continue; // predecessor unreachable
                }
                if idom[pred_rpo as usize] == UNDEFINED {
                    continue; // predecessor not yet processed
                }

                new_idom = Some(match new_idom {
                    None => pred_rpo,
                    Some(cur) => intersect(pred_rpo, cur, &idom),
                });
            }

            if let Some(new) = new_idom {
                if idom[rpo_b] != new {
                    idom[rpo_b] = new;
                    changed = true;
                }
            }
        }
    }

    eprintln!("    converged in {iterations} iterations");
    idom
}

// ── Step 6: write idom to disk ────────────────────────────────────────────────

fn write_idom(idom: &[u32], path: &Path) -> Result<()> {
    let mut w =
        BufWriter::with_capacity(IO_BUF_SIZE, File::create(path).context("create idom file")?);
    for &v in idom {
        w.write_all(&v.to_le_bytes())?;
    }
    w.flush()?;
    Ok(())
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(pass1: &Pass1Output, pass2: &Pass2Output, output_dir: &Path) -> Result<Pass3Output> {
    let t = std::time::Instant::now();
    eprintln!("  loading object index…");
    let ids = load_object_ids(&pass1.object_index_path)?;
    let n = ids.len() as u32;
    let vroot = n;
    eprintln!("    {} nodes  [{:.1}s]", n, t.elapsed().as_secs_f64());

    let t = std::time::Instant::now();
    eprintln!("  resolving GC root node indices…");
    let root_nodes = load_root_nodes(&pass1.roots, &ids);
    eprintln!(
        "    {} roots  [{:.1}s]",
        root_nodes.len(),
        t.elapsed().as_secs_f64()
    );

    let t = std::time::Instant::now();
    eprintln!("  building adjacency lists…");
    let (forward, reverse) = build_csrs(
        &pass2.edges_path,
        &pass2.reverse_edges_path,
        ids,
        &root_nodes,
        output_dir,
    )?;
    eprintln!(
        "    {} forward edges, {} reverse edges  [{:.1}s]",
        forward.neighbors.len(),
        reverse.neighbors.len(),
        t.elapsed().as_secs_f64()
    );

    let t = std::time::Instant::now();
    eprintln!("  computing RPO via DFS…");
    let (node_to_rpo, rpo_to_node) = compute_rpo(n, vroot, &root_nodes, &forward);
    eprintln!(
        "    {} reachable nodes  [{:.1}s]",
        rpo_to_node.len().saturating_sub(1),
        t.elapsed().as_secs_f64()
    );

    let t = std::time::Instant::now();
    eprintln!("  running CHK dominator algorithm…");
    let idom = compute_dominators(&rpo_to_node, &node_to_rpo, &reverse);
    eprintln!("    [{:.1}s]", t.elapsed().as_secs_f64());

    let idom_path = output_dir.join("idom.bin");
    write_idom(&idom, &idom_path)?;

    Ok(Pass3Output {
        idom_path,
        rpo_to_node,
        node_count: n,
    })
}
