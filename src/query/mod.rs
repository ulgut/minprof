//! Query layer — class histogram, top-N retained, path to GC root.
//!
//! All queries read from the on-disk index files produced by the four passes;
//! no HPROF re-parsing is required at query time.
//!
//! Output modes:
//! - Text (default): human-readable ASCII tables printed to stdout.
//! - JSON (`--json`): newline-delimited JSON objects on stdout.
//!   Progress/diagnostic messages always go to stderr regardless of mode,
//!   making stdout safe to pipe or redirect without filtering.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result};

use crate::index::{class_name, ObjectIndex, RetainedIndex};
use crate::passes::edges::{Pass2Output, EDGE_SIZE};
use crate::passes::index::{ClassDescriptorMap, Pass1Output, ENTRY_SIZE};
use crate::passes::retained::Pass4Output;

// ── Output types (used by both text and JSON renderers) ───────────────────────

pub struct AnalysisOutput {
    pub total_objects:          usize,
    pub total_classes:          usize,
    pub gc_roots:               usize,
    pub total_shallow_bytes:    u64,
    pub retained_heap_bytes:    u64,
    pub unreachable_count:      u64,
    pub unreachable_shallow:    u64,
    /// Top 20 classes by total shallow allocation.
    pub top_allocated:          Vec<ClassHistEntry>,
    /// Top 20 classes by largest single instance.
    pub top_largest:            Vec<ClassHistEntry>,
    /// Top 20 individual objects by retained heap.
    pub top_retained:           Vec<RetainedEntry>,
}

pub struct ClassHistEntry {
    pub class_name:          String,
    pub instances:           u64,
    pub total_shallow_bytes: u64,
    pub max_shallow_bytes:   u32,
}

pub struct RetainedEntry {
    pub class_name:     String,
    pub shallow_bytes:  u32,
    pub retained_bytes: u64,
}

pub struct PathStep {
    pub object_id:   u64,
    pub class_name:  String,
    pub shallow_bytes: u32,
    pub is_gc_root:  bool,
    pub is_target:   bool,
}

// ── Internal structs ──────────────────────────────────────────────────────────

#[derive(Default)]
struct ClassStats {
    count:         u64,
    total_shallow: u64,
    max_shallow:   u32,
}

struct RawRetainedRow {
    class_id: u64,
    shallow:  u32,
    retained: u64,
}

// ── Size formatting ───────────────────────────────────────────────────────────

fn fmt_size(bytes: u64) -> String {
    const MIB: u64 = 1 << 20;
    const KIB: u64 = 1 << 10;
    if bytes >= MIB {
        format!("{:.2}MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2}KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes}.00bytes")
    }
}

fn fmt_size_u32(bytes: u32) -> String {
    fmt_size(bytes as u64)
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

/// Escape a string for embedding in a JSON value.
/// Java class names contain only alphanumeric, `.`, `[`, `]`, `;`, `$`, `<`, `>` —
/// none of which require JSON escaping — but we handle the general case anyway.
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"'  => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c    => out.push(c),
        }
    }
    out.push('"');
    out
}

// ── Data collection ───────────────────────────────────────────────────────────

const TOP_N: usize = 20;

fn collect_output(
    obj_idx: &ObjectIndex,
    ret_idx: &RetainedIndex,
    class_index: &ClassDescriptorMap,
    pass1: &Pass1Output,
    pass4: &Pass4Output,
) -> Result<AnalysisOutput> {
    let mut histogram: HashMap<u64, ClassStats> = HashMap::new();
    let mut raw_retained: Vec<RawRetainedRow> = Vec::with_capacity(obj_idx.entry_count);

    for (node_idx, (_oid, cid, shallow)) in obj_idx.iter()?.enumerate() {
        let stats = histogram.entry(cid).or_default();
        stats.count += 1;
        stats.total_shallow += shallow as u64;
        stats.max_shallow = stats.max_shallow.max(shallow);
        raw_retained.push(RawRetainedRow { class_id: cid, shallow, retained: ret_idx.get(node_idx) });
    }

    let total_shallow_bytes: u64 = histogram.values().map(|s| s.total_shallow).sum();

    // Top by total allocation.
    let mut by_total: Vec<(&u64, &ClassStats)> = histogram.iter().collect();
    by_total.sort_by(|a, b| b.1.total_shallow.cmp(&a.1.total_shallow));
    let top_allocated = by_total.iter().take(TOP_N).map(|(cid, s)| ClassHistEntry {
        class_name:          class_name(**cid, class_index),
        instances:           s.count,
        total_shallow_bytes: s.total_shallow,
        max_shallow_bytes:   s.max_shallow,
    }).collect();

    // Top by largest single instance.
    let mut by_largest: Vec<(&u64, &ClassStats)> = histogram.iter().collect();
    by_largest.sort_by(|a, b| b.1.max_shallow.cmp(&a.1.max_shallow));
    let top_largest = by_largest.iter().take(TOP_N).map(|(cid, s)| ClassHistEntry {
        class_name:          class_name(**cid, class_index),
        instances:           s.count,
        total_shallow_bytes: s.total_shallow,
        max_shallow_bytes:   s.max_shallow,
    }).collect();

    // Top by retained heap.
    raw_retained.sort_unstable_by(|a, b| b.retained.cmp(&a.retained));
    let top_retained = raw_retained.iter().take(TOP_N).map(|r| RetainedEntry {
        class_name:    class_name(r.class_id, class_index),
        shallow_bytes: r.shallow,
        retained_bytes: r.retained,
    }).collect();

    Ok(AnalysisOutput {
        total_objects:       obj_idx.entry_count,
        total_classes:       pass1.class_index.len(),
        gc_roots:            pass1.roots.len(),
        total_shallow_bytes,
        retained_heap_bytes: pass4.total_heap_bytes,
        unreachable_count:   pass4.unreachable_count,
        unreachable_shallow: pass4.unreachable_shallow,
        top_allocated,
        top_largest,
        top_retained,
    })
}

// ── Text renderer ─────────────────────────────────────────────────────────────

struct Col { header: &'static str, width: usize, right: bool }

fn print_table(cols: &[Col], rows: &[Vec<String>]) {
    let sep: String = cols.iter()
        .map(|c| format!("+{:-<width$}", "", width = c.width + 2))
        .collect::<String>() + "+";
    println!("{sep}");
    let header: String = cols.iter().map(|c| {
        if c.right { format!("| {:>width$} ", c.header, width = c.width) }
        else       { format!("| {:<width$} ", c.header, width = c.width) }
    }).collect::<String>() + "|";
    println!("{header}");
    println!("{sep}");
    for row in rows {
        let line: String = cols.iter().enumerate().map(|(i, c)| {
            let val = row.get(i).map(String::as_str).unwrap_or("");
            if c.right { format!("| {:>width$} ", val, width = c.width) }
            else       { format!("| {:<width$} ", val, width = c.width) }
        }).collect::<String>() + "|";
        println!("{line}");
    }
    println!("{sep}");
}

fn emit_text(out: &AnalysisOutput) {
    println!();
    println!(
        "Found a total of {} of instances allocated on the heap ({} objects, {} classes).",
        fmt_size(out.total_shallow_bytes),
        out.total_objects,
        out.total_classes,
    );
    println!(
        "Retained heap of reachable objects: {} ({} GC roots).",
        fmt_size(out.retained_heap_bytes),
        out.gc_roots,
    );
    if out.unreachable_count > 0 {
        println!(
            "Unreachable (garbage) objects: {} objects, {} shallow — not reachable from any GC root.",
            out.unreachable_count,
            fmt_size(out.unreachable_shallow),
        );
    }

    println!();
    println!("Top {TOP_N} allocated classes:");
    println!();
    print_table(
        &[
            Col { header: "Total size", width: 12, right: true  },
            Col { header: "Instances",  width: 9,  right: true  },
            Col { header: "Largest",    width: 13, right: true  },
            Col { header: "Class name", width: 46, right: false },
        ],
        &out.top_allocated.iter().map(|e| vec![
            fmt_size(e.total_shallow_bytes),
            e.instances.to_string(),
            fmt_size_u32(e.max_shallow_bytes),
            e.class_name.clone(),
        ]).collect::<Vec<_>>(),
    );

    println!();
    println!("Top {TOP_N} largest instances:");
    println!();
    print_table(
        &[
            Col { header: "Total size", width: 13, right: true  },
            Col { header: "Instances",  width: 9,  right: true  },
            Col { header: "Largest",    width: 13, right: true  },
            Col { header: "Class name", width: 47, right: false },
        ],
        &out.top_largest.iter().map(|e| vec![
            fmt_size(e.total_shallow_bytes),
            e.instances.to_string(),
            fmt_size_u32(e.max_shallow_bytes),
            e.class_name.clone(),
        ]).collect::<Vec<_>>(),
    );

    println!();
    println!("Top {TOP_N} by retained heap:");
    println!();
    print_table(
        &[
            Col { header: "Retained",   width: 12, right: true  },
            Col { header: "Shallow",    width: 12, right: true  },
            Col { header: "Class name", width: 47, right: false },
        ],
        &out.top_retained.iter().map(|e| vec![
            fmt_size(e.retained_bytes),
            fmt_size_u32(e.shallow_bytes),
            e.class_name.clone(),
        ]).collect::<Vec<_>>(),
    );
}

// ── JSON renderer ─────────────────────────────────────────────────────────────

fn emit_json(out: &AnalysisOutput) {
    println!("{{");
    println!("  \"summary\": {{");
    println!("    \"total_objects\": {},",          out.total_objects);
    println!("    \"total_classes\": {},",          out.total_classes);
    println!("    \"gc_roots\": {},",               out.gc_roots);
    println!("    \"total_shallow_bytes\": {},",    out.total_shallow_bytes);
    println!("    \"retained_heap_bytes\": {},",    out.retained_heap_bytes);
    println!("    \"unreachable_count\": {},",      out.unreachable_count);
    println!("    \"unreachable_shallow_bytes\": {}", out.unreachable_shallow);
    println!("  }},");

    fn hist_entries(entries: &[ClassHistEntry]) -> String {
        let rows: Vec<String> = entries.iter().map(|e| format!(
            "    {{\"class_name\":{},\"instances\":{},\"total_shallow_bytes\":{},\"max_shallow_bytes\":{}}}",
            json_str(&e.class_name), e.instances, e.total_shallow_bytes, e.max_shallow_bytes,
        )).collect();
        format!("[\n{}\n  ]", rows.join(",\n"))
    }

    println!("  \"top_allocated_classes\": {},", hist_entries(&out.top_allocated));
    println!("  \"top_largest_instances\": {},",  hist_entries(&out.top_largest));

    let retained_rows: Vec<String> = out.top_retained.iter().map(|e| format!(
        "    {{\"class_name\":{},\"shallow_bytes\":{},\"retained_bytes\":{}}}",
        json_str(&e.class_name), e.shallow_bytes, e.retained_bytes,
    )).collect();
    println!("  \"top_retained\": [\n{}\n  ]", retained_rows.join(",\n"));
    println!("}}");
}

// ── Path to GC root ───────────────────────────────────────────────────────────

fn lookup_object(file: &mut File, entry_count: u64, target_id: u64) -> Result<Option<(u64, u32)>> {
    let mut lo = 0u64;
    let mut hi = entry_count;
    let mut buf = [0u8; ENTRY_SIZE];
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        file.seek(SeekFrom::Start(mid * ENTRY_SIZE as u64)).context("seek object index")?;
        file.read_exact(&mut buf).context("read object index entry")?;
        let oid = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        match oid.cmp(&target_id) {
            std::cmp::Ordering::Equal => {
                let cid = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                let sz  = u32::from_le_bytes(buf[16..20].try_into().unwrap());
                return Ok(Some((cid, sz)));
            }
            std::cmp::Ordering::Less    => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    Ok(None)
}

fn find_referrers(file: &mut File, entry_count: u64, target_id: u64) -> Result<Vec<u64>> {
    let mut lo = 0u64;
    let mut hi = entry_count;
    let mut buf = [0u8; EDGE_SIZE];
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        file.seek(SeekFrom::Start(mid * EDGE_SIZE as u64)).context("seek reverse edges")?;
        file.read_exact(&mut buf).context("read reverse edge entry")?;
        let to_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        if to_id < target_id { lo = mid + 1; } else { hi = mid; }
    }
    let mut referrers = Vec::new();
    let mut pos = lo;
    file.seek(SeekFrom::Start(pos * EDGE_SIZE as u64)).context("seek reverse edges to lower bound")?;
    loop {
        if pos >= entry_count { break; }
        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e).context("read reverse edge"),
        }
        let to_id   = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let from_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if to_id != target_id { break; }
        referrers.push(from_id);
        pos += 1;
    }
    Ok(referrers)
}

/// Find the shortest reference path from any GC root to `target_id` and emit it.
///
/// Output mode is controlled by `json`:
/// - `false` — human-readable text, e.g.:
///   ```
///   Path from GC root to 0x00000007f3a1c80:
///     0x...  java.lang.Thread       (shallow: 96.00bytes) ← GC root
///     → 0x...  com.example.Foo      (shallow: 128.00bytes) ← target
///   ```
/// - `true` — a JSON object on a single line (NDJSON-compatible):
///   ```json
///   {"type":"path_to_root","target_id":"0x...","found":true,"path":[...]}
///   ```
pub fn path_to_root(
    target_id: u64,
    pass1: &Pass1Output,
    pass2: &Pass2Output,
    json: bool,
) -> Result<()> {
    let roots: HashSet<u64> = pass1.roots.iter().copied().collect();

    let obj_entry_count = std::fs::metadata(&pass1.object_index_path)?.len() / ENTRY_SIZE as u64;
    let mut obj_file = File::open(&pass1.object_index_path)
        .context("open object index for path query")?;
    let rev_entry_count = std::fs::metadata(&pass2.reverse_edges_path)?.len() / EDGE_SIZE as u64;
    let mut rev_file = File::open(&pass2.reverse_edges_path)
        .context("open reverse edges for path query")?;

    if lookup_object(&mut obj_file, obj_entry_count, target_id)?.is_none() {
        if json {
            println!(
                "{{\"type\":\"path_to_root\",\"target_id\":\"0x{target_id:016x}\",\"found\":false,\"error\":\"object not found in index\"}}"
            );
        } else {
            println!("Object 0x{target_id:016x} not found in the object index.");
        }
        return Ok(());
    }

    // BFS over reverse reference graph from target toward roots.
    let mut prev: HashMap<u64, u64> = HashMap::new();
    prev.insert(target_id, u64::MAX);
    let mut queue: VecDeque<u64> = VecDeque::new();
    queue.push_back(target_id);

    // If the target is itself a root, the path is trivially one step.
    let found_root = if roots.contains(&target_id) {
        Some(target_id)
    } else {
        let mut found = None;
        'bfs: while let Some(obj) = queue.pop_front() {
            for referrer in find_referrers(&mut rev_file, rev_entry_count, obj)? {
                if prev.contains_key(&referrer) { continue; }
                prev.insert(referrer, obj);
                if roots.contains(&referrer) { found = Some(referrer); break 'bfs; }
                queue.push_back(referrer);
            }
        }
        found
    };

    let Some(root) = found_root else {
        if json {
            println!(
                "{{\"type\":\"path_to_root\",\"target_id\":\"0x{target_id:016x}\",\"found\":false,\"error\":\"unreachable — no GC root in reverse graph\"}}"
            );
        } else {
            println!("No path to a GC root found for 0x{target_id:016x}.");
            println!("(Object may be unreachable / garbage at dump time.)");
        }
        return Ok(());
    };

    // Reconstruct path [root, ..., target].
    let mut path_ids = vec![root];
    let mut cur = root;
    while cur != target_id {
        cur = *prev.get(&cur).unwrap();
        path_ids.push(cur);
    }

    // Resolve class names and shallow sizes for each step.
    let mut steps: Vec<PathStep> = Vec::with_capacity(path_ids.len());
    for &oid in &path_ids {
        let (cid, sz) = lookup_object(&mut obj_file, obj_entry_count, oid)?
            .unwrap_or((0, 0));
        steps.push(PathStep {
            object_id:    oid,
            class_name:   class_name(cid, &pass1.class_index),
            shallow_bytes: sz,
            is_gc_root:   oid == root,
            is_target:    oid == target_id,
        });
    }

    if json {
        let step_json: Vec<String> = steps.iter().map(|s| format!(
            "{{\"object_id\":\"0x{:016x}\",\"class_name\":{},\"shallow_bytes\":{},\"is_gc_root\":{},\"is_target\":{}}}",
            s.object_id, json_str(&s.class_name), s.shallow_bytes, s.is_gc_root, s.is_target,
        )).collect();
        println!(
            "{{\"type\":\"path_to_root\",\"target_id\":\"0x{target_id:016x}\",\"found\":true,\"path\":[{}]}}",
            step_json.join(","),
        );
    } else {
        println!();
        println!("Path from GC root to 0x{target_id:016x}:");
        println!();
        for (i, s) in steps.iter().enumerate() {
            let label  = if s.is_gc_root && s.is_target { " ← GC root + target" }
                         else if s.is_gc_root           { " ← GC root"           }
                         else if s.is_target            { " ← target"            }
                         else                           { ""                     };
            let prefix = if i == 0 { "  ".to_string() } else { "  → ".to_string() };
            println!(
                "{prefix}0x{:016x}  {:<48}  (shallow: {}){label}",
                s.object_id,
                s.class_name,
                fmt_size_u32(s.shallow_bytes),
            );
        }
        println!();
    }

    Ok(())
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Run all queries and emit results.
///
/// Progress messages go to stderr; results go to stdout.
/// When `json` is true, stdout contains a single JSON object.
pub fn run(pass1: &Pass1Output, pass4: &Pass4Output, _output_dir: &Path, json: bool) -> Result<()> {
    let obj_idx = ObjectIndex::open(&pass1.object_index_path)?;
    let ret_idx = RetainedIndex::load(&pass4.retained_path)?;
    let out = collect_output(&obj_idx, &ret_idx, &pass1.class_index, pass1, pass4)?;
    if json { emit_json(&out); } else { emit_text(&out); }
    Ok(())
}
