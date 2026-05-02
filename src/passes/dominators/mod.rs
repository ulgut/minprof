//! Pass 3 — dominator tree computation.
//!
//! Uses the Semi-NCA algorithm (Georgiadis, 2005) to compute immediate
//! dominators in O(E · α(N)) time.
//!
//! # Phases
//!
//!   Phase 1 — Semidominator computation via EVAL/LINK with iterative path
//!   compression.  Streams predecessor edges from `pred_sorted.bin`.
//!
//!   Phase 2 — Immediate dominator computation via a single forward NCA walk
//!   in DFS preorder.
//!
//! # Node numbering
//!
//! Actual objects are assigned node indices 0..N-1 corresponding to their
//! position in the sorted `object_index.bin`. A virtual root gets index N;
//! it has edges to all GC roots and dominates every node.
//!
//! # Output indexing
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

const PARTIAL_SIZE: usize = 12;
const INDEXED_SIZE: usize = 8;
const PRED_EDGE_SIZE: usize = 8;

// ── Output ────────────────────────────────────────────────────────────────────

pub struct Pass3Output {
    pub idom_path: PathBuf,
    pub rpo_to_node: Vec<u32>,
    pub node_count: u32,
}

// ── CSR (Compressed Sparse Row) adjacency list ────────────────────────────────

struct Csr {
    offsets: Vec<u32>,
    neighbors: Vec<u32>,
}

// ── Step 1: load object IDs ───────────────────────────────────────────────────

use crate::passes::index::load_object_ids;

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

// ── Step 3: build forward CSR from edge files ────────────────────────────────

fn prepare_indexed_files(
    edges_path: &Path,
    ids: Vec<u64>,
    root_nodes: &[u32],
    output_dir: &Path,
) -> Result<(PathBuf, PathBuf)> {
    let n = ids.len();
    let vroot = n as u32;

    // ── Sort 1/2: resolve from_id → from_idx, sort by to_id ────────────
    //
    // Read edges.bin (sorted by from_id). Co-scan with sorted IDs to resolve
    // from_id → from_idx.  Output 12-byte records (to_id, from_idx) sorted
    // by (to_id, from_idx).
    let partial_path = output_dir.join("partial_sorted.bin");
    {
        let mut sorter = RecordSorter::<PARTIAL_SIZE>::new(
            output_dir.to_path_buf(),
            "partial",
            key_partial,
        );
        let mut reader = BufReader::with_capacity(
            IO_BUF_SIZE,
            File::open(edges_path).context("open edges.bin")?,
        );
        let mut buf = [0u8; EDGE_SIZE];
        let mut scan = 0usize;
        while reader.read_exact(&mut buf).is_ok() {
            let from_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let to_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            while scan < n && ids[scan] < from_id {
                scan += 1;
            }
            if scan >= n || ids[scan] != from_id {
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
    eprintln!("  [adj] partial resolve done (sort 1/2)");

    // ── Sort 2/2: resolve to_id → to_idx, produce fwd + rev indexed ────
    //
    // Read partial_sorted (to_id, from_idx) sorted by to_id.  Co-scan IDs
    // to resolve to_id → to_idx.  Simultaneously:
    //   • Push (from_idx, to_idx) into fwd_indexed RecordSorter
    //   • Write (to_idx, from_idx) to rev_indexed.bin — already ordered by
    //     to_idx since we scan sorted to_ids.
    //
    // Also inject vroot→root predecessor edges into rev_indexed.
    let fwd_indexed_path = output_dir.join("fwd_indexed_sorted.bin");
    let rev_indexed_path = output_dir.join("rev_indexed.bin");
    {
        let mut fwd_sorter = RecordSorter::<INDEXED_SIZE>::new(
            output_dir.to_path_buf(),
            "fwd_indexed",
            key_indexed,
        );
        let mut rev_w = BufWriter::with_capacity(
            IO_BUF_SIZE,
            File::create(&rev_indexed_path).context("create rev_indexed")?,
        );

        let mut reader = BufReader::with_capacity(
            IO_BUF_SIZE,
            File::open(&partial_path).context("open partial sorted")?,
        );
        let mut buf = [0u8; PARTIAL_SIZE];
        let mut scan = 0usize;
        while reader.read_exact(&mut buf).is_ok() {
            let to_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let from_idx = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            while scan < n && ids[scan] < to_id {
                scan += 1;
            }
            if scan >= n || ids[scan] != to_id {
                continue;
            }
            let to_idx = scan as u32;

            let mut fwd_rec = [0u8; INDEXED_SIZE];
            fwd_rec[0..4].copy_from_slice(&from_idx.to_le_bytes());
            fwd_rec[4..8].copy_from_slice(&to_idx.to_le_bytes());
            fwd_sorter.push(fwd_rec)?;

            rev_w.write_all(&to_idx.to_le_bytes())?;
            rev_w.write_all(&from_idx.to_le_bytes())?;
        }

        // Inject vroot → root predecessor edges into reverse indexed.
        for &r in root_nodes {
            rev_w.write_all(&r.to_le_bytes())?;
            rev_w.write_all(&vroot.to_le_bytes())?;
        }

        rev_w.flush()?;
        drop(reader);
        let _ = std::fs::remove_file(&partial_path);
        drop(ids); // free 4 GB before forward merge

        fwd_sorter.finish(&fwd_indexed_path)?;
    }
    eprintln!("  [adj] indexed resolve done (sort 2/2)");

    Ok((fwd_indexed_path, rev_indexed_path))
}

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


fn build_csr_from_indexed(indexed_path: &Path, total_nodes: usize) -> Result<Csr> {
    let mut offsets = vec![0u32; total_nodes + 1];

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
        offsets[i] = offsets[i]
            .checked_add(offsets[i - 1])
            .expect("edge count overflows u32");
    }

    let edge_count = offsets[total_nodes] as usize;
    let mut neighbors = vec![0u32; edge_count];

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

// ── Step 4: DFS — output written to disk ─────────────────────────────────────

/// DFS output paths + reachable count.
struct DfsDiskOutput {
    pre_to_node_path: PathBuf,
    post_order_path: PathBuf,
    parent_pre_path: PathBuf, // parent_pre[child_pre] = parent's preorder number
    reachable: usize,         // includes vroot
}

/// Run DFS, writing all output to disk via BufWriter.
fn compute_dfs_to_disk(
    n: u32,
    vroot: u32,
    root_nodes: &[u32],
    forward: &Csr,
    output_dir: &Path,
) -> Result<DfsDiskOutput> {
    let total = (n + 1) as usize;

    let pre_path = output_dir.join("dfs_pre_to_node.bin");
    let post_path = output_dir.join("dfs_post_order.bin");
    let parent_path = output_dir.join("dfs_parent_pre.bin");

    let mut pre_w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(&pre_path).context("create pre_to_node")?,
    );
    let mut post_w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(&post_path).context("create post_order")?,
    );
    let mut parent_w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(&parent_path).context("create parent_pre")?,
    );

    // Packed-bit visited set: 62 MB for 500M nodes — fits in L3 cache.
    let mut visited: Vec<u64> = vec![0u64; (total + 63) / 64];

    // Stack frame: (node, edge_pos, edge_end, preorder_number).
    // 16 bytes per frame.  Max depth ≈ graph diameter.
    let mut stack: Vec<(u32, u32, u32, u32)> = Vec::new();
    let mut pre_counter: u32 = 0;

    macro_rules! visit_set {
        ($i:expr) => {
            visited[$i / 64] |= 1u64 << ($i % 64)
        };
    }
    macro_rules! visit_get {
        ($i:expr) => {
            (visited[$i / 64] >> ($i % 64)) & 1 != 0
        };
    }

    // Push vroot.
    visit_set!(vroot as usize);
    pre_w.write_all(&vroot.to_le_bytes())?;
    parent_w.write_all(&UNDEFINED.to_le_bytes())?; // vroot has no parent
    let vroot_pre = pre_counter;
    pre_counter += 1;
    stack.push((vroot, 0, root_nodes.len() as u32, vroot_pre));

    let mut nodes_visited = 0u64;
    while let Some(&(node, pos, end, _pre)) = stack.last() {
        if pos < end {
            let child = if node == vroot {
                root_nodes[pos as usize]
            } else {
                forward.neighbors[pos as usize]
            };
            stack.last_mut().unwrap().1 = pos + 1;

            if !visit_get!(child as usize) {
                visit_set!(child as usize);

                // Record preorder: write child node + parent's preorder.
                pre_w.write_all(&child.to_le_bytes())?;
                let parent_pre_num = stack.last().unwrap().3;
                parent_w.write_all(&parent_pre_num.to_le_bytes())?;

                let child_pre = pre_counter;
                pre_counter += 1;

                let child_start = forward.offsets[child as usize];
                let child_end = forward.offsets[child as usize + 1];
                stack.push((child, child_start, child_end, child_pre));

                nodes_visited += 1;
                if nodes_visited % 10_000_000 == 0 {
                    eprint!("\r    {nodes_visited} nodes visited...");
                }
            }
        } else {
            let (popped_node, _, _, _) = stack.pop().unwrap();
            post_w.write_all(&popped_node.to_le_bytes())?;
        }
    }
    if nodes_visited >= 10_000_000 {
        eprint!("\r                                        \r");
    }

    pre_w.flush()?;
    post_w.flush()?;
    parent_w.flush()?;

    let reachable = pre_counter as usize;

    Ok(DfsDiskOutput {
        pre_to_node_path: pre_path,
        post_order_path: post_path,
        parent_pre_path: parent_path,
        reachable,
    })
}

// ── Step 5: disk array helpers ───────────────────────────────────────────────

fn write_u32_vec(data: &[u32], path: &Path) -> Result<()> {
    let mut w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(path).context("create array file")?,
    );
    let bytes =
        unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), data.len() * 4) };
    w.write_all(bytes)?;
    w.flush()?;
    Ok(())
}

// ── Step 6: sort predecessor edges by target DFS preorder (descending) ──────

fn key_pred_desc(rec: &[u8; PRED_EDGE_SIZE]) -> (u64, u64) {
    let to_pre = u32::from_le_bytes(rec[0..4].try_into().unwrap());
    let from_pre = u32::from_le_bytes(rec[4..8].try_into().unwrap());
    ((!to_pre) as u64, from_pre as u64)
}

fn sort_pred_edges(
    rev_indexed_path: &Path,
    node_to_pre: &[u32],
    output_dir: &Path,
) -> Result<PathBuf> {
    let pred_path = output_dir.join("pred_sorted.bin");
    let mut sorter = RecordSorter::<PRED_EDGE_SIZE>::new(
        output_dir.to_path_buf(),
        "pred",
        key_pred_desc,
    );

    let mut reader = BufReader::with_capacity(
        IO_BUF_SIZE,
        File::open(rev_indexed_path).context("open rev indexed for pred sort")?,
    );
    let mut buf = [0u8; INDEXED_SIZE];
    let mut edge_count = 0u64;
    while reader.read_exact(&mut buf).is_ok() {
        let to_node = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let from_node = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let to_pre = node_to_pre[to_node as usize];
        let from_pre = node_to_pre[from_node as usize];
        if to_pre == UNDEFINED || from_pre == UNDEFINED {
            continue;
        }
        let mut rec = [0u8; PRED_EDGE_SIZE];
        rec[0..4].copy_from_slice(&to_pre.to_le_bytes());
        rec[4..8].copy_from_slice(&from_pre.to_le_bytes());
        sorter.push(rec)?;
        edge_count += 1;
    }
    sorter.finish(&pred_path)?;
    eprintln!("    {} predecessor edges sorted", edge_count);
    Ok(pred_path)
}

// ── Step 7: Semi-NCA Phase 1 — semidominator via EVAL/LINK ──────────────────
//
// The three eval arrays (semi, ancestor, label) are packed into a single
// struct so that accessing all fields of the same node hits one cache line
// (12 bytes vs 3 separate 4-byte accesses at different memory locations).
// This cuts cache misses by ~2x during path compression.

/// Packed EVAL/LINK node — 12 bytes, fits 5 per cache line.
#[derive(Clone, Copy)]
#[repr(C)]
struct EvalNode {
    semi: u32,
    ancestor: u32,
    label: u32,
}

fn snca_compress(
    nodes: &mut [EvalNode],
    stack: &mut Vec<u32>,
    v: u32,
) {
    stack.clear();
    let mut u = v;
    while nodes[u as usize].ancestor != UNDEFINED
        && nodes[nodes[u as usize].ancestor as usize].ancestor != UNDEFINED
    {
        stack.push(u);
        u = nodes[u as usize].ancestor;
    }

    while let Some(w) = stack.pop() {
        let wi = w as usize;
        let anc = nodes[wi].ancestor;
        let ai = anc as usize;
        if nodes[nodes[ai].label as usize].semi < nodes[nodes[wi].label as usize].semi {
            nodes[wi].label = nodes[ai].label;
        }
        nodes[wi].ancestor = nodes[ai].ancestor;
    }
}

#[inline(always)]
fn snca_eval(
    nodes: &mut [EvalNode],
    stack: &mut Vec<u32>,
    v: u32,
) -> u32 {
    if nodes[v as usize].ancestor == UNDEFINED {
        v
    } else {
        snca_compress(nodes, stack, v);
        nodes[v as usize].label
    }
}

fn compute_semidominators(
    pred_sorted_path: &Path,
    parent_pre: &[u32],
    reachable: usize,
) -> Result<Vec<EvalNode>> {
    let mut nodes: Vec<EvalNode> = (0..reachable as u32)
        .map(|i| EvalNode {
            semi: i,
            ancestor: UNDEFINED,
            label: i,
        })
        .collect();
    let mut compress_stack: Vec<u32> = Vec::new();

    let mut reader = BufReader::with_capacity(
        IO_BUF_SIZE,
        File::open(pred_sorted_path).context("open pred sorted")?,
    );
    let mut buf = [0u8; PRED_EDGE_SIZE];
    let mut have_edge = reader.read_exact(&mut buf).is_ok();
    let mut edge_to_pre = if have_edge {
        u32::from_le_bytes(buf[0..4].try_into().unwrap())
    } else {
        0
    };
    let mut edge_from_pre = if have_edge {
        u32::from_le_bytes(buf[4..8].try_into().unwrap())
    } else {
        0
    };

    for w_pre in (1..reachable as u32).rev() {
        while have_edge && edge_to_pre == w_pre {
            let v_pre = edge_from_pre;
            let u_pre = snca_eval(&mut nodes, &mut compress_stack, v_pre);
            if nodes[u_pre as usize].semi < nodes[w_pre as usize].semi {
                nodes[w_pre as usize].semi = nodes[u_pre as usize].semi;
            }

            have_edge = reader.read_exact(&mut buf).is_ok();
            if have_edge {
                edge_to_pre = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                edge_from_pre = u32::from_le_bytes(buf[4..8].try_into().unwrap());
            }
        }

        nodes[w_pre as usize].ancestor = parent_pre[w_pre as usize];

        if w_pre % 10_000_000 == 0 {
            eprint!(
                "\r    phase 1: {} / {} nodes...",
                reachable as u32 - w_pre,
                reachable
            );
        }
    }
    if reachable > 10_000_000 {
        eprint!("\r                                              \r");
    }

    Ok(nodes)
}

// ── Step 8: Semi-NCA Phase 2 — NCA idom walk ────────────────────────────────

fn compute_idom_nca(
    nodes: &[EvalNode],
    parent_pre: &[u32],
    reachable: usize,
) -> Vec<u32> {
    let mut idom_pre: Vec<u32> = vec![UNDEFINED; reachable];
    idom_pre[0] = 0;

    for w_pre in 1..reachable as u32 {
        let mut x = parent_pre[w_pre as usize];
        while x > nodes[w_pre as usize].semi {
            x = idom_pre[x as usize];
        }
        idom_pre[w_pre as usize] = x;

        if w_pre % 10_000_000 == 0 {
            eprint!("\r    phase 2: {} / {} nodes...", w_pre, reachable);
        }
    }
    if reachable > 10_000_000 {
        eprint!("\r                                              \r");
    }

    idom_pre
}

// ── Step 9: write idom to disk ───────────────────────────────────────────────

// ── Public entry point ────────────────────────────────────────────────────────

pub fn run(pass1: &Pass1Output, pass2: &Pass2Output, output_dir: &Path) -> Result<Pass3Output> {
    // ── Load IDs and roots ───────────────────────────────────────────────────
    let t = std::time::Instant::now();
    eprintln!("  loading object index...");
    let ids = load_object_ids(&pass1.object_index_path)?;
    let n = ids.len() as u32;
    let vroot = n;
    eprintln!("    {} nodes  [{:.1}s]", n, t.elapsed().as_secs_f64());

    let t = std::time::Instant::now();
    eprintln!("  resolving GC root node indices...");
    let root_nodes = load_root_nodes(&pass1.roots, &ids);
    eprintln!(
        "    {} roots  [{:.1}s]",
        root_nodes.len(),
        t.elapsed().as_secs_f64()
    );

    // ── Produce indexed files while ids is in memory ─────────────────────────
    let t = std::time::Instant::now();
    eprintln!("  building adjacency lists...");
    let (fwd_indexed, rev_indexed) = prepare_indexed_files(
        &pass2.edges_path,
        ids,
        &root_nodes,
        output_dir,
    )?;

    // ── Build forward CSR for DFS ────────────────────────────────────────────
    let forward = build_csr_from_indexed(&fwd_indexed, n as usize)?;
    let _ = std::fs::remove_file(&fwd_indexed);
    let fwd_edge_count = forward.neighbors.len();
    eprintln!(
        "    {} forward edges  [{:.1}s]",
        fwd_edge_count,
        t.elapsed().as_secs_f64()
    );

    // ── DFS — output written to disk ─────────────────────────────────────────
    let t = std::time::Instant::now();
    eprintln!("  computing DFS spanning tree...");
    let dfs = compute_dfs_to_disk(n, vroot, &root_nodes, &forward, output_dir)?;
    let reachable = dfs.reachable;
    drop(forward);
    eprintln!(
        "    {} reachable nodes  [{:.1}s]",
        reachable.saturating_sub(1),
        t.elapsed().as_secs_f64()
    );

    // ── Build node_to_pre from pre_to_node on disk ───────────────────────────
    // parent_pre is already on disk in the right format (indexed by preorder).
    let t = std::time::Instant::now();
    eprintln!("  building node_to_pre...");
    let total = (n + 1) as usize;
    {
        // Read pre_to_node from disk, build node_to_pre in memory.
        let pre_to_node = crate::passes::read_u32s(&dfs.pre_to_node_path)?;
        let mut node_to_pre = vec![UNDEFINED; total];
        for (pre, &node) in pre_to_node.iter().enumerate() {
            node_to_pre[node as usize] = pre as u32;
        }
        // pre_to_node is on disk already; drop the in-memory copy.
        drop(pre_to_node);

        // Build rpo_to_node from post_order on disk.
        let mut post_order = crate::passes::read_u32s(&dfs.post_order_path)?;
        let _ = std::fs::remove_file(&dfs.post_order_path);
        post_order.reverse();
        let rpo_to_node = post_order;
        let rpo_to_node_path = output_dir.join("rpo_to_node.bin");
        write_u32_vec(&rpo_to_node, &rpo_to_node_path)?;
        drop(rpo_to_node);

        eprintln!("    [{:.1}s]", t.elapsed().as_secs_f64());

        // ── Sort predecessor edges by target preorder (descending) ───────────
        let t = std::time::Instant::now();
        eprintln!("  sorting predecessor edges by DFS preorder...");
        let pred_sorted_path = sort_pred_edges(&rev_indexed, &node_to_pre, output_dir)?;
        let _ = std::fs::remove_file(&rev_indexed);
        drop(node_to_pre);
        eprintln!("    [{:.1}s]", t.elapsed().as_secs_f64());

        // ── Phase 1: compute semidominators ──────────────────────────────────
        let t = std::time::Instant::now();
        eprintln!("  computing dominators (Semi-NCA)...");
        let parent_pre = crate::passes::read_u32s(&dfs.parent_pre_path)?;
        let _ = std::fs::remove_file(&dfs.parent_pre_path);

        eprintln!("    phase 1: computing semidominators...");
        let eval_nodes = compute_semidominators(&pred_sorted_path, &parent_pre, reachable)?;
        let _ = std::fs::remove_file(&pred_sorted_path);

        // ── Phase 2: compute idom via NCA walk ───────────────────────────────
        eprintln!("    phase 2: computing immediate dominators...");
        let idom_pre = compute_idom_nca(&eval_nodes, &parent_pre, reachable);
        drop(eval_nodes);
        drop(parent_pre);
        eprintln!("    [{:.1}s]", t.elapsed().as_secs_f64());

        // ── Convert idom from preorder to RPO ────────────────────────────────
        let t = std::time::Instant::now();
        eprintln!("  converting idom to RPO...");
        let pre_to_node = crate::passes::read_u32s(&dfs.pre_to_node_path)?;
        let _ = std::fs::remove_file(&dfs.pre_to_node_path);
        let rpo_to_node = crate::passes::read_u32s(&rpo_to_node_path)?;

        let mut node_to_rpo = vec![UNDEFINED; total];
        for (rpo, &node) in rpo_to_node.iter().enumerate() {
            node_to_rpo[node as usize] = rpo as u32;
        }

        let rpo_count = rpo_to_node.len();
        let mut idom_rpo = vec![UNDEFINED; rpo_count];
        idom_rpo[0] = 0;
        for w_pre in 1..reachable as u32 {
            let w_node = pre_to_node[w_pre as usize];
            let w_rpo = node_to_rpo[w_node as usize];
            let dom_pre = idom_pre[w_pre as usize];
            let dom_node = pre_to_node[dom_pre as usize];
            let dom_rpo = node_to_rpo[dom_node as usize];
            idom_rpo[w_rpo as usize] = dom_rpo;
        }
        drop(idom_pre);
        drop(pre_to_node);
        drop(node_to_rpo);
        eprintln!("    [{:.1}s]", t.elapsed().as_secs_f64());

        // ── Write idom to disk ───────────────────────────────────────────────
        let idom_path = output_dir.join("idom.bin");
        write_u32_vec(&idom_rpo, &idom_path).context("write idom.bin")?;
        let _ = std::fs::remove_file(&rpo_to_node_path);

        Ok(Pass3Output {
            idom_path,
            rpo_to_node,
            node_count: n,
        })
    }
}
