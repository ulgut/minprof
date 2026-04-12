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

use crate::passes::edges::{Pass2Output, EDGE_SIZE};
use crate::passes::index::{Pass1Output, ENTRY_SIZE};

// ── Constants ─────────────────────────────────────────────────────────────────

const UNDEFINED: u32 = u32::MAX;

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
    offsets: Vec<u32>,
    neighbors: Vec<u32>,
}

impl Csr {
    fn neighbors(&self, node: u32) -> &[u32] {
        let start = self.offsets[node as usize] as usize;
        let end   = self.offsets[node as usize + 1] as usize;
        &self.neighbors[start..end]
    }
}

// ── Step 1: load object IDs ───────────────────────────────────────────────────

/// Load the sorted object ID array from `object_index.bin`.
/// Position i in this array is the node index for that object.
fn load_object_ids(index_path: &Path) -> Result<Vec<u64>> {
    let file_len = std::fs::metadata(index_path)?.len() as usize;
    let n = file_len / ENTRY_SIZE;
    let mut ids = Vec::with_capacity(n);

    let mut reader = BufReader::new(File::open(index_path).context("open object index")?);
    let mut buf = [0u8; ENTRY_SIZE];
    while reader.read_exact(&mut buf).is_ok() {
        let object_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        ids.push(object_id);
    }
    Ok(ids)
}

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

// ── Step 3: build forward and reverse CSR from edge file ─────────────────────

/// Read the edge file and build both forward (successor) and reverse
/// (predecessor) CSR adjacency lists in one pass.
///
/// The virtual root (node N) is inserted as a predecessor of each GC root
/// in the reverse CSR so the algorithm has a well-defined entry point.
fn build_csrs(
    edges_path: &Path,
    ids: &[u64],
    root_nodes: &[u32],
) -> Result<(Csr, Csr)> {
    let n = ids.len() as u32;
    let vroot = n; // virtual root index

    // -- Collect all edges as (from_idx, to_idx) pairs ----------------------
    let edge_file_len = std::fs::metadata(edges_path)?.len() as usize;
    let edge_count = edge_file_len / EDGE_SIZE;

    let mut raw_forward: Vec<(u32, u32)> = Vec::with_capacity(edge_count);
    let mut reader = BufReader::new(File::open(edges_path).context("open edges file")?);
    let mut buf = [0u8; EDGE_SIZE];

    while reader.read_exact(&mut buf).is_ok() {
        let from_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let to_id   = u64::from_le_bytes(buf[8..16].try_into().unwrap());

        let (Some(from_idx), Some(to_idx)) = (lookup_node(ids, from_id), lookup_node(ids, to_id)) else {
            continue; // skip edges to objects not in the index
        };
        raw_forward.push((from_idx, to_idx));
    }

    // -- Add virtual root → GC root edges -----------------------------------
    // These appear only in the reverse CSR as (gc_root, vroot) pairs.
    let vroot_edges: Vec<(u32, u32)> = root_nodes
        .iter()
        .map(|&r| (vroot, r))
        .collect();

    // -- Build forward CSR (actual object edges only) ----------------------
    // raw_forward is already sorted by from_idx (inherited from edges.bin
    // which is sorted by from_id, and lookup preserves order).
    let forward = build_csr_from_sorted(n, &raw_forward);

    // -- Build reverse CSR (predecessors) -----------------------------------
    // Collect all backward edges + virtual-root predecessor edges.
    let mut raw_reverse: Vec<(u32, u32)> = raw_forward
        .iter()
        .map(|&(from, to)| (to, from))
        .chain(vroot_edges.into_iter().map(|(from, to)| (to, from)))
        .collect();
    raw_reverse.sort_unstable_by_key(|&(to, _)| to);

    // Reverse CSR has N+1 nodes (0..N actual + vroot N).
    let reverse = build_csr_from_sorted(n + 1, &raw_reverse);

    Ok((forward, reverse))
}

/// Build a CSR from a list of `(source, target)` pairs **sorted by source**.
fn build_csr_from_sorted(node_count: u32, sorted_edges: &[(u32, u32)]) -> Csr {
    let n = node_count as usize;
    let mut offsets = vec![0u32; n + 1];

    // Count out-degree per node.
    for &(src, _) in sorted_edges {
        offsets[src as usize + 1] += 1;
    }
    // Prefix sum.
    for i in 1..=n {
        offsets[i] += offsets[i - 1];
    }

    // Fill neighbor list.
    let mut neighbors = vec![0u32; sorted_edges.len()];
    let mut cursor = offsets.clone();
    for &(src, dst) in sorted_edges {
        let pos = cursor[src as usize] as usize;
        neighbors[pos] = dst;
        cursor[src as usize] += 1;
    }

    Csr { offsets, neighbors }
}

// ── Step 4: iterative DFS for RPO numbering ───────────────────────────────────

/// Compute RPO numbering via iterative DFS from the virtual root.
///
/// Returns `(node_to_rpo, rpo_to_node)`:
/// - `node_to_rpo[i]`  = RPO number of node i (`UNDEFINED` if unreachable)
/// - `rpo_to_node[j]`  = node index with RPO number j
fn compute_rpo(
    n: u32,
    vroot: u32,
    root_nodes: &[u32],
    forward: &Csr,
) -> (Vec<u32>, Vec<u32>) {
    let total = (n + 1) as usize; // actual nodes + virtual root
    let mut node_to_rpo = vec![UNDEFINED; total];
    let mut visited     = vec![false; total];
    let mut post_order: Vec<u32> = Vec::with_capacity(total);

    // Iterative DFS stack: (node_idx, successor_cursor).
    let mut stack: Vec<(u32, usize)> = Vec::new();

    visited[vroot as usize] = true;
    stack.push((vroot, 0));

    while !stack.is_empty() {
        let top_idx = stack.len() - 1;
        let (node, cursor) = stack[top_idx];

        // Successors of vroot are the GC root nodes; others use the forward CSR.
        let next = if node == vroot {
            root_nodes.get(cursor).copied()
        } else {
            forward.neighbors(node).get(cursor).copied()
        };

        if let Some(succ) = next {
            stack[top_idx].1 += 1; // advance cursor before potential push
            if !visited[succ as usize] {
                visited[succ as usize] = true;
                stack.push((succ, 0));
            }
        } else {
            // All successors explored — record post-order finish.
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
fn compute_dominators(
    rpo_to_node: &[u32],
    node_to_rpo: &[u32],
    reverse: &Csr,
) -> Vec<u32> {
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
                    None      => pred_rpo,
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
    let mut w = BufWriter::new(File::create(path).context("create idom file")?);
    for &v in idom {
        w.write_all(&v.to_le_bytes())?;
    }
    w.flush()?;
    Ok(())
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(
    pass1: &Pass1Output,
    pass2: &Pass2Output,
    output_dir: &Path,
) -> Result<Pass3Output> {
    let t = std::time::Instant::now();
    eprintln!("  loading object index…");
    let ids = load_object_ids(&pass1.object_index_path)?;
    let n = ids.len() as u32;
    let vroot = n;
    eprintln!("    {} nodes  [{:.1}s]", n, t.elapsed().as_secs_f64());

    let t = std::time::Instant::now();
    eprintln!("  resolving GC root node indices…");
    let root_nodes = load_root_nodes(&pass1.roots, &ids);
    eprintln!("    {} roots  [{:.1}s]", root_nodes.len(), t.elapsed().as_secs_f64());

    let t = std::time::Instant::now();
    eprintln!("  building adjacency lists…");
    let (forward, reverse) = build_csrs(&pass2.edges_path, &ids, &root_nodes)?;
    eprintln!("    {} forward edges, {} reverse edges  [{:.1}s]",
        forward.neighbors.len(), reverse.neighbors.len(), t.elapsed().as_secs_f64());

    let t = std::time::Instant::now();
    eprintln!("  computing RPO via DFS…");
    let (node_to_rpo, rpo_to_node) = compute_rpo(n, vroot, &root_nodes, &forward);
    eprintln!("    {} reachable nodes  [{:.1}s]",
        rpo_to_node.len().saturating_sub(1), t.elapsed().as_secs_f64());

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
