//! Pass 4 — retained size computation.
//!
//! Walks the dominator tree bottom-up (highest RPO number to lowest) and
//! accumulates shallow sizes into retained sizes. A node's retained size is
//! its own shallow size plus the retained sizes of everything it dominates.
//!
//! # Algorithm
//!
//! For each node j processed in reverse RPO order (deepest → shallowest):
//!   `retained[idom[j]] += retained[j]`
//!
//! This is O(N) — a single pass over the RPO array.
//!
//! # Output
//!
//! `retained.bin`: a flat array of `u64`, one entry per node index
//! (position in `object_index.bin`). Index with node_idx to get retained bytes.
//! The virtual root is excluded — only actual objects (indices 0..N-1) are written.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::passes::dominators::Pass3Output;
use crate::passes::index::{ENTRY_SIZE, Pass1Output};

const UNDEFINED: u32 = u32::MAX;

use crate::passes::IO_BUF_SIZE;

// ── Output ────────────────────────────────────────────────────────────────────

pub struct Pass4Output {
    /// Retained size array on disk: `u64` values indexed by node_idx.
    /// `retained_path[i]` = retained bytes of the object at position i
    /// in `object_index.bin`.
    pub retained_path: PathBuf,
    /// Sum of all retained sizes (should equal total heap bytes).
    pub total_heap_bytes: u64,
    pub node_count: u32,
    /// Number of objects not reachable from any GC root.
    pub unreachable_count: u64,
    /// Sum of shallow sizes of unreachable objects.
    pub unreachable_shallow: u64,
}

// ── Step 1: load shallow sizes ────────────────────────────────────────────────

/// Read the `shallow_size` field from every entry in `object_index.bin`.
/// Returns a Vec indexed by node_idx (same order as the file).
fn load_shallow_sizes(index_path: &Path) -> Result<Vec<u32>> {
    let file_len = std::fs::metadata(index_path)?.len() as usize;
    let n = file_len / ENTRY_SIZE;
    let mut sizes = Vec::with_capacity(n);

    let mut reader = BufReader::with_capacity(
        IO_BUF_SIZE,
        File::open(index_path).context("open object index")?,
    );
    let mut buf = [0u8; ENTRY_SIZE];

    while reader.read_exact(&mut buf).is_ok() {
        // object_index entry: object_id(8) + class_id(8) + shallow_size(4)
        let shallow = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        sizes.push(shallow);
    }

    Ok(sizes)
}

// ── Step 2: read idom array ───────────────────────────────────────────────────

fn load_idom(idom_path: &Path) -> Result<Vec<u32>> {
    let file_len = std::fs::metadata(idom_path)?.len() as usize;
    let count = file_len / 4;
    let mut idom = Vec::with_capacity(count);

    let mut reader = BufReader::with_capacity(
        IO_BUF_SIZE,
        File::open(idom_path).context("open idom file")?,
    );
    let mut buf = [0u8; 4];
    while reader.read_exact(&mut buf).is_ok() {
        idom.push(u32::from_le_bytes(buf));
    }
    Ok(idom)
}

// ── Step 3: compute retained sizes ───────────────────────────────────────────

/// Compute retained sizes by walking the dominator tree bottom-up.
///
/// `retained` is indexed by node_idx (0..N for actual objects, N for vroot).
/// Returns `(retained, unreachable_count, unreachable_shallow)`.
fn compute_retained(
    shallow: &[u32],     // indexed by node_idx, length N
    idom: &[u32],        // indexed by RPO number, length = reachable node count
    rpo_to_node: &[u32], // RPO number → node_idx (includes vroot at RPO 0)
    node_count: u32,     // N: number of actual objects
) -> (Vec<u64>, u64, u64) {
    let n = node_count as usize;

    // Initialise retained[i] = shallow_size[i] for actual objects.
    // retained[n] (index past the last real object) is the virtual root slot.
    let mut retained: Vec<u64> = shallow.iter().map(|&s| s as u64).collect();
    retained.push(0u64); // slot for the virtual root

    // Track which nodes were reached during the RPO traversal.
    // Nodes not in rpo_to_node are unreachable (no idom entry).
    let mut reachable = vec![false; n];
    for &node in &rpo_to_node[1..] {
        // rpo_to_node[0] is the virtual root; skip it (index N, not a real object)
        let idx = node as usize;
        if idx < n {
            reachable[idx] = true;
        }
    }

    // Walk RPO from the deepest node up to (but not including) the virtual root.
    // rpo_to_node[0] = vroot; rpo_to_node[1..] = actual nodes in RPO order.
    // Processing in reverse (highest RPO first) guarantees that when we
    // propagate node j's retained size into idom[j], node j is fully
    // accumulated (all nodes it dominates have already been folded in).
    for rpo in (1..rpo_to_node.len()).rev() {
        let node = rpo_to_node[rpo] as usize;
        let dom_rpo = idom[rpo];

        if dom_rpo == UNDEFINED {
            continue; // unreachable node — no dominator
        }

        let dom_node = rpo_to_node[dom_rpo as usize] as usize;

        // Safety: dom_node is either a real object (0..N-1) or the vroot (N).
        // All are valid indices in `retained`.
        let node_retained = retained[node];
        retained[dom_node] += node_retained;
    }

    // Count unreachable objects and sum their shallow sizes.
    let mut unreachable_count = 0u64;
    let mut unreachable_shallow = 0u64;
    for i in 0..n {
        if !reachable[i] {
            unreachable_count += 1;
            unreachable_shallow += shallow[i] as u64;
        }
    }

    (retained, unreachable_count, unreachable_shallow)
}

// ── Step 4: write retained sizes ─────────────────────────────────────────────

fn write_retained(retained: &[u64], node_count: usize, path: &Path) -> Result<()> {
    let mut w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(path).context("create retained file")?,
    );
    // Write only actual objects (0..N), exclude the virtual root slot at N.
    for &r in &retained[..node_count] {
        w.write_all(&r.to_le_bytes())?;
    }
    w.flush()?;
    Ok(())
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(pass1: &Pass1Output, pass3: &Pass3Output, output_dir: &Path) -> Result<Pass4Output> {
    let n = pass3.node_count as usize;

    let shallow = load_shallow_sizes(&pass1.object_index_path)?;
    assert_eq!(shallow.len(), n, "shallow size count mismatch");

    let idom = load_idom(&pass3.idom_path)?;

    let (retained, unreachable_count, unreachable_shallow) =
        compute_retained(&shallow, &idom, &pass3.rpo_to_node, pass3.node_count);

    // retained[N] = virtual root's retained size = total heap bytes.
    let total_heap_bytes = retained[n];

    let retained_path = output_dir.join("retained.bin");
    write_retained(&retained, n, &retained_path)?;

    eprintln!(
        "  total heap {:.1} MiB across {} objects ({} unreachable, {:.1} MiB garbage)",
        total_heap_bytes as f64 / 1_048_576.0,
        n,
        unreachable_count,
        unreachable_shallow as f64 / 1_048_576.0,
    );

    Ok(Pass4Output {
        retained_path,
        total_heap_bytes,
        node_count: pass3.node_count,
        unreachable_count,
        unreachable_shallow,
    })
}
