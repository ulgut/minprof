//! Pass 3 — dominator tree computation.
//!
//! Uses the Lengauer-Tarjan (LT) algorithm, which computes dominators in a
//! single DFS pass plus amortised-near-linear work via LINK-EVAL path
//! compression — O(E α(E,V)) vs the iterative CHK alternative.
//!
//! # Node numbering
//!
//! Actual objects are assigned node indices 0..N-1 corresponding to their
//! position in the sorted `object_index.bin`. A virtual root gets index N;
//! it has edges to all GC roots and dominates every node.
//!
//! # RPO indexing
//!
//! LT uses DFS pre-numbering; the DFS spanning tree is identical to what a
//! post-order DFS reversed (RPO) would produce.  Lower RPO number = higher
//! in the dominator tree (virtual root = RPO 0).
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
    /// u32 suffices because practical heap dumps have far fewer than u32::MAX edges.
    offsets: Vec<u32>,
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

/// Produce the on-disk indexed files for both CSRs while `ids` is loaded,
/// then drop `ids` before the caller materialises any CSR.
/// Returns paths to the two indexed files (sorted by node index).
fn prepare_indexed_files(
    edges_path: &Path,
    rev_edges_path: &Path,
    ids: Vec<u64>,
    root_nodes: &[u32],
    output_dir: &Path,
) -> Result<(PathBuf, PathBuf)> {
    let vroot = ids.len() as u32;
    let fwd_indexed = resolve_forward_indexed(edges_path, &ids, output_dir)?;
    let rev_indexed =
        resolve_reverse_indexed(rev_edges_path, &ids, root_nodes, vroot, output_dir)?;
    drop(ids); // free object index before any CSR is allocated
    Ok((fwd_indexed, rev_indexed))
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
    let mut offsets = vec![0u32; total_nodes + 1];

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
        offsets[i] = offsets[i].checked_add(offsets[i - 1])
            .expect("edge count overflows u32; file has more than u32::MAX edges");
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

// ── Step 4: DFS for RPO numbering + DFS-tree parent recording ────────────────

/// Compute reverse post-order (RPO) via iterative DFS from the virtual root,
/// and simultaneously record the DFS-tree parent and DFS preorder (discovery
/// order) of every reachable node.
///
/// Returns `(node_to_rpo, rpo_to_node, parent_node, node_to_pre, pre_to_node)`:
/// - `node_to_rpo[i]`  = RPO number of node i (`UNDEFINED` if unreachable)
/// - `rpo_to_node[j]`  = node index at RPO position j
/// - `parent_node[i]`  = node index of i's parent in the DFS spanning tree
///                       (`UNDEFINED` for the virtual root and unreachable nodes)
/// - `node_to_pre[i]`  = DFS preorder (discovery order) number of node i
/// - `pre_to_node[p]`  = node index at preorder position p
///
/// RPO and preorder are both topological orderings of the DFS spanning tree
/// (parent always has lower number than children), but they differ for nodes
/// in different subtrees.  The LT algorithm's ancestor check (`dfnum(v) <
/// dfnum(w)`) requires preorder numbers, not RPO.  RPO is used for the
/// external interface (`idom.bin` is RPO-indexed) and for ordering the main
/// LT loop (reverse RPO ensures all descendants are processed before a node).
///
/// The explicit DFS stack stores `(node: u32, cursor: u32)` — 8 bytes per
/// frame.  Stack depth equals the longest DFS path (typically thousands of
/// entries for real Java heaps), keeping peak RSS well below the forward-CSR
/// footprint that dominates during this phase.
fn compute_dfs(
    n: u32,
    vroot: u32,
    root_nodes: &[u32],
    forward: &Csr,
) -> (Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>) {
    let total = (n + 1) as usize; // actual nodes + virtual root

    // Packed-bit visited set: 8× smaller than Vec<bool>.
    let mut visited: Vec<u64> = vec![0u64; (total + 63) / 64];
    let mut stack: Vec<(u32, u32)> = Vec::new();
    let mut post_order: Vec<u32> = Vec::with_capacity(total);
    // parent_node[v] = DFS-tree parent of v (UNDEFINED for vroot / unreachable).
    let mut parent_node = vec![UNDEFINED; total];
    // Preorder (discovery order): assigned when a node is first pushed.
    let mut node_to_pre = vec![UNDEFINED; total];
    let mut pre_to_node: Vec<u32> = Vec::with_capacity(total);

    macro_rules! visit_set {
        ($i:expr) => { visited[$i / 64] |= 1u64 << ($i % 64) };
    }
    macro_rules! visit_get {
        ($i:expr) => { (visited[$i / 64] >> ($i % 64)) & 1 != 0 };
    }

    // Assign preorder on first discovery (push).
    let assign_pre = |node: u32, node_to_pre: &mut Vec<u32>, pre_to_node: &mut Vec<u32>| {
        node_to_pre[node as usize] = pre_to_node.len() as u32;
        pre_to_node.push(node);
    };

    visit_set!(vroot as usize);
    assign_pre(vroot, &mut node_to_pre, &mut pre_to_node);
    stack.push((vroot, 0));

    while !stack.is_empty() {
        let &(node, cursor) = stack.last().unwrap();
        let neighbors: &[u32] = if node == vroot {
            root_nodes
        } else {
            forward.neighbors(node)
        };

        if (cursor as usize) < neighbors.len() {
            let child = neighbors[cursor as usize];
            stack.last_mut().unwrap().1 += 1;
            if !visit_get!(child as usize) {
                visit_set!(child as usize);
                parent_node[child as usize] = node; // record DFS-tree parent
                assign_pre(child, &mut node_to_pre, &mut pre_to_node);
                stack.push((child, 0));
            }
        } else {
            stack.pop();
            post_order.push(node);
        }
    }

    post_order.reverse(); // RPO = reversed post-order

    drop(visited);
    drop(stack);

    let mut node_to_rpo = vec![UNDEFINED; total];
    for (rpo, &node) in post_order.iter().enumerate() {
        node_to_rpo[node as usize] = rpo as u32;
    }

    (node_to_rpo, post_order, parent_node, node_to_pre, pre_to_node)
}

// ── Step 5: Lengauer-Tarjan dominator algorithm ────────────────────────────────
//
// Reference: Lengauer & Tarjan, "A fast algorithm for finding dominators in a
// flowgraph", TOPLAS 1979.  This is the "simple" (path-compressed) variant:
// O(E log V) worst-case, O(E α(E,V)) in practice.
//
// Key structures (all indexed by node index, size = n+1):
//   semi[v]      – DFS number of v's semidominator (initialised to dfnum[v])
//   idom_node[v] – immediate dominator of v, stored as a node index
//   ancestor[v]  – LINK-EVAL forest link (UNDEFINED = forest root)
//   label[v]     – node with min semi[] on the compressed path from v upward
//   bucket[v]    – nodes whose semidominator resolves to v

/// EVAL: return the non-root vertex with minimum `semi` on the path from `v`
/// to the root of its tree in the LINK-EVAL forest.  Returns `v` itself when
/// `v` is a forest root.
fn lt_eval(ancestor: &mut [u32], label: &mut [u32], semi: &[u32], v: u32) -> u32 {
    if ancestor[v as usize] == UNDEFINED {
        return v;
    }
    lt_compress(ancestor, label, semi, v);
    label[v as usize]
}

/// COMPRESS: iterative path compression for the LINK-EVAL forest.
///
/// Collect the path from `v` upward until we reach a node whose parent is a
/// forest root (i.e. `ancestor[ancestor[cur]] == UNDEFINED`).  Then process
/// the path from the top downward, updating `label` and collapsing `ancestor`
/// pointers so that future EVAL calls skip intermediate nodes in O(α) time.
fn lt_compress(ancestor: &mut [u32], label: &mut [u32], semi: &[u32], v: u32) {
    // Collect path: stop when cur's grandparent is a forest root.
    let mut path = vec![v];
    let mut cur = v;
    loop {
        let a = ancestor[cur as usize];
        if a == UNDEFINED {
            break; // cur is itself a root (safety guard; shouldn't occur in practice)
        }
        if ancestor[a as usize] == UNDEFINED {
            break; // cur's parent is a root — the recursive base case is a no-op here
        }
        cur = a;
        path.push(cur);
    }
    // Process from the second-to-last element down to path[0] = v.
    // (The last element's compress is a no-op per the recursive definition.)
    for i in (0..path.len().saturating_sub(1)).rev() {
        let w = path[i];
        let anc = ancestor[w as usize];
        if semi[label[anc as usize] as usize] < semi[label[w as usize] as usize] {
            label[w as usize] = label[anc as usize];
        }
        // Path compression: point w directly to anc's (already-updated) parent.
        ancestor[w as usize] = ancestor[anc as usize];
    }
}

/// Compute the immediate dominator for every reachable node using
/// Lengauer-Tarjan.  One forward pass (step 2) in reverse DFS order plus one
/// correction pass (step 3) in forward DFS order — no iteration to convergence.
///
/// **Preorder vs RPO**: The LT ancestor check `dfnum(v) < dfnum(w)` is only
/// sound with DFS *preorder* (discovery-order) numbers.  RPO (post-order
/// reversed) can rank a sibling subtree explored later as "lower" than nodes
/// in an earlier subtree, incorrectly treating it as an ancestor.  We use
/// preorder for `semi[]` and the ancestor test, but keep reverse-RPO as the
/// main loop ordering (reverse-RPO still ensures all descendants are processed
/// before ancestors).  RPO is used only for the external output (`idom.bin`).
///
/// Returns `idom_rpo` indexed by RPO number; each value is the RPO number of
/// the immediate dominator. Virtual root (RPO 0) maps to itself.
fn compute_dominators_lt(
    rpo_to_node: &[u32],
    node_to_rpo: &[u32],
    parent_node: &[u32],
    node_to_pre: &[u32],   // DFS preorder number per node
    pre_to_node: &[u32],   // node at preorder position p
    reverse: &Csr,
    n: u32,
) -> Vec<u32> {
    let rpo_count = rpo_to_node.len();
    let total = (n + 1) as usize;

    // semi[v] = DFS *preorder* number of v's semidominator; initialised to pre[v].
    // Using preorder ensures that `pre[v] < pre[w]` iff v is a proper DFS ancestor of w,
    // which is required by the LT semidominator computation in step 2a.
    let mut semi = node_to_pre.to_vec();

    // idom_node[v] = immediate dominator of v as a node index.
    let mut idom_node = vec![UNDEFINED; total];

    // LINK-EVAL forest.
    let mut ancestor = vec![UNDEFINED; total];
    let mut label: Vec<u32> = (0..total as u32).collect(); // label[v] = v initially

    // Bucket implemented as two intrusive linked lists sharing the same node-index
    // space.  Using `Vec<Vec<u32>>` would allocate O(N) heap objects (24 bytes each
    // even when empty), which is prohibitive at hundreds of millions of nodes.
    //
    // `bucket_head[v]`  = head of the singly-linked list for bucket(v), or UNDEFINED.
    // `bucket_next[v]`  = next node in the same bucket list as v, or UNDEFINED.
    //
    // Total overhead: 2 × u32 per node = 8 bytes/node instead of 24 bytes/node.
    let mut bucket_head = vec![UNDEFINED; total];
    let mut bucket_next = vec![UNDEFINED; total];

    let pre_count = pre_to_node.len(); // number of reachable nodes (including vroot)

    // ── Step 2: reverse DFS *preorder* (skip vroot at preorder 0) ────────────
    //
    // The loop must process vertices in reverse preorder (not reverse RPO).
    // The key invariant: when processing w (preorder p_w), all vertices v with
    // preorder(v) > p_w have already been LINKed into the LINK-EVAL forest.
    // This ensures EVAL(v) correctly reflects v's minimum-semi ancestor for
    // non-ancestor predecessors of w.
    //
    // Reverse RPO violates this: a non-ancestor v in a "later-explored" sibling
    // subtree has preorder(v) > preorder(w) but can have RPO(v) < RPO(w),
    // causing it to be LINKed AFTER w is processed and making EVAL(v) stale.
    for pre_w_idx in (1..pre_count).rev() {
        let w = pre_to_node[pre_w_idx] as usize;
        let p_w = parent_node[w];
        let pre_w = pre_w_idx as u32; // = node_to_pre[w]

        // Step 2a: semidominator of w.
        for &v in reverse.neighbors(w as u32) {
            let pre_v = node_to_pre[v as usize];
            if pre_v == UNDEFINED {
                continue; // unreachable predecessor — ignore
            }
            // Ancestor check uses DFS *preorder*: if pre(v) < pre(w), v was
            // discovered before w in the DFS, making v a proper DFS ancestor of w
            // (in directed DFS, discovering v before w via the edge v→w implies
            // ancestry).  For non-ancestors (pre_v > pre_w), the semidominator
            // candidate comes from EVAL — the min-semi vertex on the path above v
            // in the LINK-EVAL forest.
            //
            // Note: we deliberately do NOT use RPO here.  Lower RPO only means
            // "finishes later", and a node in a sibling subtree explored after w
            // can have lower RPO than w yet still not be a DFS ancestor.
            let u = if pre_v < pre_w {
                v
            } else {
                lt_eval(&mut ancestor, &mut label, &semi, v)
            };
            if semi[u as usize] < semi[w] {
                semi[w] = semi[u as usize];
            }
        }

        // Step 2b: w's semidominator resolves at pre_to_node[semi[w]]; enqueue w.
        // semi[w] is now a preorder number; pre_to_node maps it back to a node index.
        let semi_node = pre_to_node[semi[w] as usize] as usize;
        bucket_next[w] = bucket_head[semi_node];
        bucket_head[semi_node] = w as u32;

        // Step 2c: LINK(parent[w], w) — add w to the forest under its parent.
        if p_w != UNDEFINED {
            ancestor[w] = p_w;
        }

        // Step 2d: process all nodes in bucket[parent[w]] and clear it.
        if p_w != UNDEFINED {
            let p = p_w as usize;
            let mut cur = bucket_head[p];
            bucket_head[p] = UNDEFINED;
            while cur != UNDEFINED {
                let v = cur as usize;
                cur = bucket_next[v];
                bucket_next[v] = UNDEFINED;
                let u = lt_eval(&mut ancestor, &mut label, &semi, v as u32) as usize;
                // If semi[u] < semi[v], idom[v] will be refined in step 3.
                idom_node[v] = if semi[u] < semi[v] { u as u32 } else { p as u32 };
            }
        }
    }

    // ── Step 3: correction pass in forward DFS order ─────────────────────────
    //
    // The virtual root (index n) is the DFS tree root.  Its idom_node was
    // never set in step 2 (the main loop skips RPO 0).  Seed it with itself
    // so that any chain reaching vroot in step 3 terminates correctly instead
    // of propagating UNDEFINED.
    idom_node[n as usize] = n;

    // Forward preorder ensures parents are processed before children, so that
    // when we follow idom[d] for the tentative idom d, idom[d] is already final.
    for pre_w_idx in 1..pre_count {
        let w = pre_to_node[pre_w_idx] as usize;
        // semi[w] is a preorder number; use pre_to_node to get the semidominator node.
        let semi_node = pre_to_node[semi[w] as usize] as usize;
        let d = idom_node[w];
        if d != UNDEFINED && d as usize != semi_node {
            // idom[w] was set tentatively; resolve it one hop further.
            idom_node[w] = idom_node[d as usize];
        }
    }

    // ── Convert: node-indexed idom → RPO-indexed idom ────────────────────────
    let mut idom_rpo = vec![UNDEFINED; rpo_count];
    idom_rpo[0] = 0; // virtual root dominates itself

    for rpo_w in 1..rpo_count {
        let w = rpo_to_node[rpo_w] as usize;
        let d = idom_node[w];
        if d != UNDEFINED {
            idom_rpo[rpo_w] = node_to_rpo[d as usize];
        }
    }

    idom_rpo
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
    // ── Load IDs and roots ────────────────────────────────────────────────────
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

    // ── Produce indexed files for both CSRs while ids is in memory ───────────
    let t = std::time::Instant::now();
    eprintln!("  building adjacency lists…");
    let (fwd_indexed, rev_indexed) = prepare_indexed_files(
        &pass2.edges_path,
        &pass2.reverse_edges_path,
        ids, // consumed here; dropped inside before any CSR is allocated
        &root_nodes,
        output_dir,
    )?;

    // ── Phase 1: forward CSR + RPO ────────────────────────────────────────────
    // Reverse CSR is not allocated yet, keeping peak RSS well under total RAM.
    let forward = build_csr_from_indexed(&fwd_indexed, n as usize, 0)?;
    let _ = std::fs::remove_file(&fwd_indexed);
    eprintln!(
        "    {} forward edges  [{:.1}s]",
        forward.neighbors.len(),
        t.elapsed().as_secs_f64()
    );

    let t = std::time::Instant::now();
    eprintln!("  computing DFS spanning tree…");
    let (node_to_rpo, rpo_to_node, parent_node, node_to_pre, pre_to_node) =
        compute_dfs(n, vroot, &root_nodes, &forward);
    eprintln!(
        "    {} reachable nodes  [{:.1}s]",
        rpo_to_node.len().saturating_sub(1),
        t.elapsed().as_secs_f64()
    );
    drop(forward); // forward CSR no longer needed; free before reverse CSR is allocated

    // ── Phase 2: reverse CSR + Lengauer-Tarjan ────────────────────────────────
    let t = std::time::Instant::now();
    eprintln!("  building reverse adjacency list…");
    let reverse = build_csr_from_indexed(&rev_indexed, n as usize + 1, 0)?;
    let _ = std::fs::remove_file(&rev_indexed);
    eprintln!(
        "    {} reverse edges  [{:.1}s]",
        reverse.neighbors.len(),
        t.elapsed().as_secs_f64()
    );

    let t = std::time::Instant::now();
    eprintln!("  running Lengauer-Tarjan dominator algorithm…");
    let idom = compute_dominators_lt(
        &rpo_to_node, &node_to_rpo, &parent_node, &node_to_pre, &pre_to_node, &reverse, n,
    );
    eprintln!("    [{:.1}s]", t.elapsed().as_secs_f64());

    let idom_path = output_dir.join("idom.bin");
    write_idom(&idom, &idom_path)?;

    Ok(Pass3Output {
        idom_path,
        rpo_to_node,
        node_count: n,
    })
}
