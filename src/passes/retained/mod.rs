//! Pass 4 — retained size computation.
//!
//! Walks the dominator tree bottom-up (highest RPO number to lowest) and
//! accumulates shallow sizes into retained sizes. A node's retained size is
//! its own shallow size plus the retained sizes of everything it dominates.
//!
//! # Algorithm
//!
//! The computation works entirely in RPO space to minimize random memory
//! accesses.  `retained_rpo[rpo]` holds the retained size for the node at
//! that RPO position.  The main loop:
//!
//!   `retained_rpo[idom[rpo]] += retained_rpo[rpo]`
//!
//! has only **one** random access (the write to `retained_rpo[idom[rpo]]`).
//! Both `idom[rpo]` and `retained_rpo[rpo]` are read sequentially as `rpo`
//! decreases.  This is 3x fewer random accesses than working in node-index
//! space where every iteration requires `rpo_to_node` lookups.
//!
//! # Output
//!
//! `retained.bin`: a flat array of `u64`, one entry per node index
//! (position in `object_index.bin`). Index with node_idx to get retained bytes.
//! The virtual root is excluded — only actual objects (indices 0..N-1) are written.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::passes::dominators::Pass3Output;
use crate::passes::index::Pass1Output;

const UNDEFINED: u32 = u32::MAX;

use crate::passes::IO_BUF_SIZE;

// ── Output ────────────────────────────────────────────────────────────────────

pub struct Pass4Output {
    /// Retained size array on disk: `u64` values indexed by node_idx.
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

/// Read `shallow_sizes.bin`: a compact array of `u32`, one per object.
/// Written by pass 1 during the merge step
fn load_shallow_sizes(path: &Path) -> Result<Vec<u32>> {
    let file_len = std::fs::metadata(path)?.len() as usize;
    let n = file_len / 4;
    let mut sizes = Vec::with_capacity(n);

    let mut reader = BufReader::with_capacity(
        IO_BUF_SIZE,
        File::open(path).context("open shallow_sizes.bin")?,
    );
    loop {
        let consumed = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let records = buf.len() / 4;
            for chunk in buf[..records * 4].chunks_exact(4) {
                sizes.push(u32::from_le_bytes(chunk.try_into().unwrap()));
            }
            records * 4
        };
        reader.consume(consumed);
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
    loop {
        let consumed = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let records = buf.len() / 4;
            for chunk in buf[..records * 4].chunks_exact(4) {
                idom.push(u32::from_le_bytes(chunk.try_into().unwrap()));
            }
            records * 4
        };
        reader.consume(consumed);
    }
    Ok(idom)
}

// ── Step 3: compute retained sizes in RPO space ─────────────────────────────

/// Compute retained sizes working entirely in RPO space.
///
/// Returns `(retained_rpo, total_shallow, unreachable_count, unreachable_shallow)`.
/// `retained_rpo[rpo]` = retained size for the node at that RPO position.
fn compute_retained_rpo(
    shallow: &[u32],     // indexed by node_idx, length N
    idom: &[u32],        // indexed by RPO number
    rpo_to_node: &[u32], // RPO number → node_idx (RPO 0 = vroot)
    node_count: u32,
) -> (Vec<u64>, u64, u64, u64) {
    let n = node_count as usize;
    let reachable = rpo_to_node.len();

    // Total shallow sum (all objects, including unreachable).
    let mut total_shallow = 0u64;
    for &s in shallow {
        total_shallow += s as u64;
    }

    // Initialize retained_rpo from shallow sizes.
    // RPO 0 = vroot (shallow = 0).
    let mut retained_rpo: Vec<u64> = Vec::with_capacity(reachable);
    retained_rpo.push(0u64);
    for &node in &rpo_to_node[1..] {
        retained_rpo.push(shallow[node as usize] as u64);
    }

    // Walk RPO in reverse (deepest → shallowest).
    // Sequential reads: idom[rpo], retained_rpo[rpo].
    // Single random write: retained_rpo[dom_rpo].
    for rpo in (1..reachable).rev() {
        let dom_rpo = idom[rpo];
        if dom_rpo == UNDEFINED {
            continue;
        }
        retained_rpo[dom_rpo as usize] += retained_rpo[rpo];
    }

    // Unreachable stats (avoids a bitmap — just subtract).
    let reachable_count = (reachable - 1) as u64;
    let unreachable_count = n as u64 - reachable_count;
    let reachable_shallow: u64 = rpo_to_node[1..]
        .iter()
        .map(|&node| shallow[node as usize] as u64)
        .sum();
    let unreachable_shallow = total_shallow - reachable_shallow;

    (retained_rpo, total_shallow, unreachable_count, unreachable_shallow)
}

// ── Step 4: write retained.bin (node-indexed) ────────────────────────────────

/// Write retained.bin indexed by node_idx.
///
/// Builds `node_to_rpo` for the lookup.  Reachable nodes get their computed
/// retained size; unreachable nodes get their shallow size.
fn write_retained(
    retained_rpo: &[u64],
    shallow: &[u32],
    rpo_to_node: &[u32],
    node_count: usize,
    path: &Path,
) -> Result<()> {
    let total = node_count + 1; // +1 for vroot
    let mut node_to_rpo = vec![UNDEFINED; total];
    for (rpo, &node) in rpo_to_node.iter().enumerate() {
        node_to_rpo[node as usize] = rpo as u32;
    }

    let mut w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(path).context("create retained file")?,
    );

    // Write in batches to reduce per-entry overhead.
    // 8192 entries × 8 bytes = 64 KiB batch — fits in L1.
    const BATCH: usize = 8192;
    let mut buf = [0u8; BATCH * 8];
    let mut i = 0usize;
    while i < node_count {
        let end = (i + BATCH).min(node_count);
        let count = end - i;
        for j in 0..count {
            let node = i + j;
            let rpo = node_to_rpo[node];
            let r = if rpo != UNDEFINED {
                retained_rpo[rpo as usize]
            } else {
                shallow[node] as u64
            };
            buf[j * 8..(j + 1) * 8].copy_from_slice(&r.to_le_bytes());
        }
        w.write_all(&buf[..count * 8])?;
        i = end;
    }
    w.flush()?;
    Ok(())
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(pass1: &Pass1Output, pass3: &Pass3Output, output_dir: &Path) -> Result<Pass4Output> {
    let t = std::time::Instant::now();
    let n = pass3.node_count as usize;

    eprintln!("  loading shallow sizes...");
    let shallow = load_shallow_sizes(&pass1.shallow_sizes_path)?;
    assert_eq!(shallow.len(), n, "shallow size count mismatch");

    eprintln!("  loading idom...");
    let idom = load_idom(&pass3.idom_path)?;

    eprintln!("  computing retained sizes...");
    let (retained_rpo, _total_shallow, unreachable_count, unreachable_shallow) =
        compute_retained_rpo(&shallow, &idom, &pass3.rpo_to_node, pass3.node_count);

    let total_heap_bytes = retained_rpo[0]; // vroot retained = total reachable heap
    drop(idom);

    eprintln!("  writing retained.bin...");
    let retained_path = output_dir.join("retained.bin");
    write_retained(
        &retained_rpo,
        &shallow,
        &pass3.rpo_to_node,
        n,
        &retained_path,
    )?;
    drop(retained_rpo);
    drop(shallow);

    eprintln!(
        "  total heap {:.1} MiB across {} objects ({} unreachable, {:.1} MiB garbage)  [{:.1}s]",
        total_heap_bytes as f64 / 1_048_576.0,
        n,
        unreachable_count,
        unreachable_shallow as f64 / 1_048_576.0,
        t.elapsed().as_secs_f64(),
    );

    Ok(Pass4Output {
        retained_path,
        total_heap_bytes,
        node_count: pass3.node_count,
        unreachable_count,
        unreachable_shallow,
    })
}
