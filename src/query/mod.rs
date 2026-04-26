//! Query layer — class histogram, retained heap by class, leak suspects,
//! package summary, path to GC root, and HTML report.
//!
//! All queries read from the on-disk index files produced by the four passes;
//! no HPROF re-parsing is required at query time.
//!
//! Output modes:
//! - Text (default): human-readable ASCII tables printed to stdout.
//! - JSON (`--json`): newline-delimited JSON objects on stdout.
//!   Progress/diagnostic messages always go to stderr regardless of mode,
//!   making stdout safe to pipe or redirect without filtering.

mod html;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result};

use crate::index::{ObjectIndex, class_name};
use crate::passes::edges::{EDGE_SIZE, Pass2Output};
use crate::passes::index::{ClassDescriptorMap, ENTRY_SIZE, Pass1Output};
use crate::passes::retained::Pass4Output;

// ── Report selection ──────────────────────────────────────────────────────────

/// Which analyses to run. All fields default to false; use `ReportConfig::all()`
/// or set individual flags.
#[derive(Debug, Clone, Default)]
pub struct ReportConfig {
    /// Class histogram: top N by total shallow allocation and by largest instance.
    pub histogram: bool,
    /// Retained heap aggregated by class: which classes hold the most memory alive.
    pub retained_by_class: bool,
    /// Leak suspects: classes retaining a large fraction of the heap.
    pub leak_suspects: bool,
    /// Package-level summary: retained heap grouped by Java package.
    pub package_summary: bool,
}

impl ReportConfig {
    pub fn all() -> Self {
        Self {
            histogram: true,
            retained_by_class: true,
            leak_suspects: true,
            package_summary: true,
        }
    }
}

// ── Output types ──────────────────────────────────────────────────────────────

pub struct AnalysisOutput {
    pub total_objects: usize,
    pub total_classes: usize,
    pub gc_roots: usize,
    pub total_shallow_bytes: u64,
    pub retained_heap_bytes: u64,
    pub unreachable_count: u64,
    pub unreachable_shallow: u64,
    /// Top 20 classes by total shallow allocation.
    pub top_allocated: Vec<ClassHistEntry>,
    /// Top 20 classes by largest single instance.
    pub top_largest: Vec<ClassHistEntry>,
    /// Top 20 individual objects by retained heap. Object IDs can be passed to --path.
    pub top_retained_objects: Vec<RetainedObjectEntry>,
    /// Top 20 classes by total retained heap.
    pub retained_by_class: Vec<RetainedByClassEntry>,
    /// Top 20 Java packages by total retained heap.
    pub package_summary: Vec<PackageSummaryEntry>,
    /// Classes retaining ≥ 1% of heap (Eclipse MAT "Leak Suspects").
    pub leak_suspects: Vec<LeakSuspectEntry>,
    /// Full package hierarchy for the HTML treemap (all packages, not truncated).
    pub treemap_data: Vec<TreemapPackage>,
    // ── GC pressure metrics (Eclipse MAT "System Overview") ──────────────────
    pub finalizer_queue_depth: u64,
    pub soft_ref_count: u64,
    pub weak_ref_count: u64,
    pub phantom_ref_count: u64,
}

pub struct ClassHistEntry {
    pub class_name: String,
    pub instances: u64,
    pub total_shallow_bytes: u64,
    pub max_shallow_bytes: u32,
}

pub struct RetainedObjectEntry {
    pub object_id: u64,
    pub class_name: String,
    pub shallow_bytes: u32,
    pub retained_bytes: u64,
}

pub struct RetainedByClassEntry {
    pub class_name: String,
    pub instance_count: u64,
    pub total_retained_bytes: u64,
    pub total_shallow_bytes: u64,
}

pub struct PackageSummaryEntry {
    pub package: String,
    pub class_count: u64,
    pub instance_count: u64,
    pub total_shallow_bytes: u64,
    pub total_retained_bytes: u64,
}

/// A classified leak suspect — class retaining ≥ 1% of heap.
#[allow(dead_code)]
pub struct LeakSuspectEntry {
    pub class_name: String,
    pub instance_count: u64,
    pub total_retained_bytes: u64,
    pub total_shallow_bytes: u64,
    pub avg_retained_bytes: u64,
    pub pct_of_heap: f64,
    pub pattern: &'static str,
}

/// Package node in the treemap hierarchy.
pub struct TreemapPackage {
    pub name: String,
    pub retained_bytes: u64,
    pub classes: Vec<TreemapClass>,
}

pub struct TreemapClass {
    pub name: String,
    pub retained_bytes: u64,
    pub instance_count: u64,
}

pub struct PathStep {
    pub object_id: u64,
    pub class_name: String,
    pub shallow_bytes: u32,
    pub is_gc_root: bool,
    pub is_target: bool,
}

// ── Internal collection types ─────────────────────────────────────────────────

#[derive(Default)]
struct ClassStats {
    count: u64,
    total_shallow: u64,
    max_shallow: u32,
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

fn fmt_pct(part: u64, total: u64) -> String {
    if total == 0 {
        return "  0.0%".to_string();
    }
    format!("{:5.1}%", part as f64 / total as f64 * 100.0)
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// ── Package extraction ────────────────────────────────────────────────────────

/// Extract the Java package from a display class name.
///
/// | Class name                  | Package               |
/// |-----------------------------|-----------------------|
/// | `com.example.foo.Bar`       | `com.example.foo`     |
/// | `java.util.HashMap`         | `java.util`           |
/// | `java.util.HashMap[]`       | `java.util`           |
/// | `byte[]`, `int`             | `<primitives>`        |
/// | `java.lang.Class`           | `java.lang`           |
/// | `<unknown@0x...>`           | `<unknown>`           |
fn package_of(class_name: &str) -> String {
    if class_name.starts_with('<') {
        return "<unknown>".to_string();
    }
    // Strip trailing array dimensions (e.g. "Foo[][]" → "Foo")
    let base = class_name.trim_end_matches(|c: char| c == '[' || c == ']');
    if !base.contains('.') {
        return "<primitive arrays>".to_string();
    }
    match base.rfind('.') {
        Some(pos) => base[..pos].to_string(),
        None => base.to_string(),
    }
}

// ── Data collection ───────────────────────────────────────────────────────────

const TOP_N: usize = 20;

fn collect_output(
    obj_idx: &ObjectIndex,
    retained_path: &Path,
    class_index: &ClassDescriptorMap,
    pass1: &Pass1Output,
    pass4: &Pass4Output,
) -> Result<AnalysisOutput> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // Single streaming pass over object_index.bin + retained.bin.
    // Computes histogram, retained-by-class, and top-N objects simultaneously
    // with O(classes) memory instead of O(objects).
    let mut histogram: HashMap<u64, ClassStats> = HashMap::new();
    let mut retained_by_class_map: HashMap<u64, (u64, u64, u64)> = HashMap::new(); // (retained, shallow, count)

    // Min-heap of size TOP_N for top retained objects.  Reverse so the
    // smallest retained value sits at the top and can be evicted cheaply.
    let mut top_heap: BinaryHeap<Reverse<(u64, u64)>> = BinaryHeap::new(); // (retained, object_id)
    let mut top_rows: HashMap<u64, (u64, u64, u32)> = HashMap::new(); // oid → (cid, retained, shallow)

    // Stream retained.bin alongside object_index.bin — never load 4 GB.
    let mut ret_reader = BufReader::with_capacity(
        crate::passes::IO_BUF_SIZE,
        File::open(retained_path).context("open retained.bin for query")?,
    );
    let mut ret_buf = [0u8; 8];

    for (oid, cid, shallow) in obj_idx.iter()? {
        let retained = if ret_reader.read_exact(&mut ret_buf).is_ok() {
            u64::from_le_bytes(ret_buf)
        } else {
            shallow as u64
        };

        // Histogram
        let stats = histogram.entry(cid).or_default();
        stats.count += 1;
        stats.total_shallow += shallow as u64;
        stats.max_shallow = stats.max_shallow.max(shallow);

        // Retained by class
        let e = retained_by_class_map.entry(cid).or_default();
        e.0 += retained;
        e.1 += shallow as u64;
        e.2 += 1;

        // Top-N retained objects via bounded min-heap
        if top_heap.len() < TOP_N {
            top_heap.push(Reverse((retained, oid)));
            top_rows.insert(oid, (cid, retained, shallow));
        } else if let Some(&Reverse((min_ret, _))) = top_heap.peek() {
            if retained > min_ret {
                let Reverse((_, evicted_oid)) = top_heap.pop().unwrap();
                top_rows.remove(&evicted_oid);
                top_heap.push(Reverse((retained, oid)));
                top_rows.insert(oid, (cid, retained, shallow));
            }
        }
    }

    let total_shallow_bytes: u64 = histogram.values().map(|s| s.total_shallow).sum();

    // ── Top by total allocation ───────────────────────────────────────────────
    let mut by_total: Vec<(&u64, &ClassStats)> = histogram.iter().collect();
    by_total.sort_by(|a, b| {
        b.1.total_shallow
            .cmp(&a.1.total_shallow)
            .then_with(|| class_name(*a.0, class_index).cmp(&class_name(*b.0, class_index)))
    });
    let top_allocated = by_total
        .iter()
        .take(TOP_N)
        .map(|(cid, s)| ClassHistEntry {
            class_name: class_name(**cid, class_index),
            instances: s.count,
            total_shallow_bytes: s.total_shallow,
            max_shallow_bytes: s.max_shallow,
        })
        .collect();

    // ── Top by largest single instance ────────────────────────────────────────
    let mut by_largest: Vec<(&u64, &ClassStats)> = histogram.iter().collect();
    by_largest.sort_by(|a, b| {
        b.1.max_shallow
            .cmp(&a.1.max_shallow)
            .then_with(|| class_name(*a.0, class_index).cmp(&class_name(*b.0, class_index)))
    });
    let top_largest = by_largest
        .iter()
        .take(TOP_N)
        .map(|(cid, s)| ClassHistEntry {
            class_name: class_name(**cid, class_index),
            instances: s.count,
            total_shallow_bytes: s.total_shallow,
            max_shallow_bytes: s.max_shallow,
        })
        .collect();

    // ── Top individual objects by retained heap ───────────────────────────────
    let mut top_retained_objects: Vec<RetainedObjectEntry> = top_heap
        .into_sorted_vec()
        .into_iter()
        .map(|Reverse((_, oid))| {
            let &(cid, retained, shallow) = top_rows.get(&oid).unwrap();
            RetainedObjectEntry {
                object_id: oid,
                class_name: class_name(cid, class_index),
                shallow_bytes: shallow,
                retained_bytes: retained,
            }
        })
        .collect();
    top_retained_objects.sort_by(|a, b| {
        b.retained_bytes
            .cmp(&a.retained_bytes)
            .then_with(|| a.object_id.cmp(&b.object_id))
    });

    // ── Retained heap aggregated by class ─────────────────────────────────────
    let mut retained_by_class_full: Vec<RetainedByClassEntry> = retained_by_class_map
        .iter()
        .map(|(cid, (total_ret, total_sh, count))| RetainedByClassEntry {
            class_name: class_name(*cid, class_index),
            instance_count: *count,
            total_retained_bytes: *total_ret,
            total_shallow_bytes: *total_sh,
        })
        .collect();
    retained_by_class_full.sort_by(|a, b| {
        b.total_retained_bytes
            .cmp(&a.total_retained_bytes)
            .then_with(|| a.class_name.cmp(&b.class_name))
    });

    // ── Package summary + treemap data (computed from full retained-by-class) ──
    let package_summary = compute_package_summary(&retained_by_class_full);
    let treemap_data = compute_treemap_data(&retained_by_class_full);

    // ── Leak suspects ─────────────────────────────────────────────────────────
    let threshold = (total_shallow_bytes / 100).max(1);
    let leak_suspects: Vec<LeakSuspectEntry> = retained_by_class_full
        .iter()
        .filter(|e| e.total_retained_bytes >= threshold)
        .map(|e| {
            let avg = if e.instance_count > 0 {
                e.total_retained_bytes / e.instance_count
            } else {
                0
            };
            let pct = e.total_retained_bytes as f64 / total_shallow_bytes as f64 * 100.0;
            LeakSuspectEntry {
                class_name: e.class_name.clone(),
                instance_count: e.instance_count,
                total_retained_bytes: e.total_retained_bytes,
                total_shallow_bytes: e.total_shallow_bytes,
                avg_retained_bytes: avg,
                pct_of_heap: pct,
                pattern: classify_suspect(e.instance_count, avg, e.total_retained_bytes),
            }
        })
        .collect();

    // ── GC pressure metrics ───────────────────────────────────────────────────
    let mut finalizer_queue_depth = 0u64;
    let mut soft_ref_count = 0u64;
    let mut weak_ref_count = 0u64;
    let mut phantom_ref_count = 0u64;
    for e in &retained_by_class_full {
        match e.class_name.as_str() {
            "java.lang.ref.Finalizer" => finalizer_queue_depth = e.instance_count,
            "java.lang.ref.SoftReference" => soft_ref_count = e.instance_count,
            "java.lang.ref.WeakReference" => weak_ref_count = e.instance_count,
            "java.lang.ref.PhantomReference" => phantom_ref_count = e.instance_count,
            _ => {}
        }
    }

    // Truncate retained_by_class to TOP_N for text/JSON output.
    retained_by_class_full.truncate(TOP_N);

    Ok(AnalysisOutput {
        total_objects: obj_idx.entry_count,
        total_classes: pass1.class_index.len(),
        gc_roots: pass1.roots.len(),
        total_shallow_bytes,
        retained_heap_bytes: pass4.total_heap_bytes,
        unreachable_count: pass4.unreachable_count,
        unreachable_shallow: pass4.unreachable_shallow,
        top_allocated,
        top_largest,
        top_retained_objects,
        retained_by_class: retained_by_class_full,
        package_summary,
        leak_suspects,
        treemap_data,
        finalizer_queue_depth,
        soft_ref_count,
        weak_ref_count,
        phantom_ref_count,
    })
}

fn compute_treemap_data(retained_by_class: &[RetainedByClassEntry]) -> Vec<TreemapPackage> {
    let mut by_pkg: HashMap<String, (u64, Vec<TreemapClass>)> = HashMap::new();
    for e in retained_by_class {
        let pkg = package_of(&e.class_name);
        let entry = by_pkg.entry(pkg).or_insert_with(|| (0, Vec::new()));
        entry.0 += e.total_retained_bytes;
        entry.1.push(TreemapClass {
            name: e.class_name.clone(),
            retained_bytes: e.total_retained_bytes,
            instance_count: e.instance_count,
        });
    }
    let mut pkgs: Vec<TreemapPackage> = by_pkg
        .into_iter()
        .map(|(name, (retained_bytes, mut classes))| {
            classes.sort_by(|a, b| {
                b.retained_bytes
                    .cmp(&a.retained_bytes)
                    .then_with(|| a.name.cmp(&b.name))
            });
            TreemapPackage {
                name,
                retained_bytes,
                classes,
            }
        })
        .collect();
    pkgs.sort_by(|a, b| {
        b.retained_bytes
            .cmp(&a.retained_bytes)
            .then_with(|| a.name.cmp(&b.name))
    });
    pkgs
}

fn compute_package_summary(retained_by_class: &[RetainedByClassEntry]) -> Vec<PackageSummaryEntry> {
    let mut by_pkg: HashMap<String, PackageSummaryEntry> = HashMap::new();
    for entry in retained_by_class {
        let pkg = package_of(&entry.class_name);
        let e = by_pkg
            .entry(pkg.clone())
            .or_insert_with(|| PackageSummaryEntry {
                package: pkg,
                class_count: 0,
                instance_count: 0,
                total_shallow_bytes: 0,
                total_retained_bytes: 0,
            });
        e.class_count += 1;
        e.instance_count += entry.instance_count;
        e.total_shallow_bytes += entry.total_shallow_bytes;
        e.total_retained_bytes += entry.total_retained_bytes;
    }
    let mut result: Vec<PackageSummaryEntry> = by_pkg.into_values().collect();
    result.sort_by(|a, b| {
        b.total_retained_bytes
            .cmp(&a.total_retained_bytes)
            .then_with(|| a.package.cmp(&b.package))
    });
    result.truncate(TOP_N);
    result
}

// ── Table renderer ────────────────────────────────────────────────────────────

struct Col {
    header: &'static str,
    width: usize,
    right: bool,
}

fn print_table(cols: &[Col], rows: &[Vec<String>]) {
    let sep: String = cols
        .iter()
        .map(|c| format!("+{:-<width$}", "", width = c.width + 2))
        .collect::<String>()
        + "+";
    println!("{sep}");
    let header: String = cols
        .iter()
        .map(|c| {
            if c.right {
                format!("| {:>width$} ", c.header, width = c.width)
            } else {
                format!("| {:<width$} ", c.header, width = c.width)
            }
        })
        .collect::<String>()
        + "|";
    println!("{header}");
    println!("{sep}");
    for row in rows {
        let line: String = cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let val = row.get(i).map(String::as_str).unwrap_or("");
                if c.right {
                    format!("| {:>width$} ", val, width = c.width)
                } else {
                    format!("| {:<width$} ", val, width = c.width)
                }
            })
            .collect::<String>()
            + "|";
        println!("{line}");
    }
    println!("{sep}");
}

// ── Text renderer ─────────────────────────────────────────────────────────────

fn emit_text(out: &AnalysisOutput, config: &ReportConfig) {
    // ── Summary (always shown) ────────────────────────────────────────────────
    println!();
    println!(
        "Total heap: {} shallow across {} objects ({} classes, {} GC roots).",
        fmt_size(out.total_shallow_bytes),
        out.total_objects,
        out.total_classes,
        out.gc_roots,
    );
    println!(
        "Retained heap (reachable): {}.",
        fmt_size(out.retained_heap_bytes),
    );
    if out.unreachable_count > 0 {
        println!(
            "Unreachable (garbage): {} objects, {} — not held by any GC root.",
            out.unreachable_count,
            fmt_size(out.unreachable_shallow),
        );
    }

    // ── Class histogram ───────────────────────────────────────────────────────
    if config.histogram {
        println!();
        println!("── Top {TOP_N} classes by total allocation ──");
        println!();
        print_table(
            &[
                Col {
                    header: "Total",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Instances",
                    width: 9,
                    right: true,
                },
                Col {
                    header: "Largest",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Class",
                    width: 48,
                    right: false,
                },
            ],
            &out.top_allocated
                .iter()
                .map(|e| {
                    vec![
                        fmt_size(e.total_shallow_bytes),
                        e.instances.to_string(),
                        fmt_size_u32(e.max_shallow_bytes),
                        e.class_name.clone(),
                    ]
                })
                .collect::<Vec<_>>(),
        );

        println!();
        println!("── Top {TOP_N} classes by largest single instance ──");
        println!();
        print_table(
            &[
                Col {
                    header: "Total",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Instances",
                    width: 9,
                    right: true,
                },
                Col {
                    header: "Largest",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Class",
                    width: 48,
                    right: false,
                },
            ],
            &out.top_largest
                .iter()
                .map(|e| {
                    vec![
                        fmt_size(e.total_shallow_bytes),
                        e.instances.to_string(),
                        fmt_size_u32(e.max_shallow_bytes),
                        e.class_name.clone(),
                    ]
                })
                .collect::<Vec<_>>(),
        );
    }

    // ── Retained heap by class ────────────────────────────────────────────────
    if config.retained_by_class {
        println!();
        println!("── Top {TOP_N} classes by retained heap ──");
        println!();
        print_table(
            &[
                Col {
                    header: "Retained",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "% heap",
                    width: 7,
                    right: true,
                },
                Col {
                    header: "Shallow",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Instances",
                    width: 9,
                    right: true,
                },
                Col {
                    header: "Avg/inst",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Class",
                    width: 40,
                    right: false,
                },
            ],
            &out.retained_by_class
                .iter()
                .map(|e| {
                    let avg = if e.instance_count > 0 {
                        e.total_retained_bytes / e.instance_count
                    } else {
                        0
                    };
                    vec![
                        fmt_size(e.total_retained_bytes),
                        fmt_pct(e.total_retained_bytes, out.total_shallow_bytes),
                        fmt_size(e.total_shallow_bytes),
                        e.instance_count.to_string(),
                        fmt_size(avg),
                        e.class_name.clone(),
                    ]
                })
                .collect::<Vec<_>>(),
        );

        println!();
        println!("── Top {TOP_N} individual objects by retained heap ──");
        println!("   (copy an object ID and run: minprof <heap> --path <ID>)");
        println!();
        print_table(
            &[
                Col {
                    header: "Retained",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "% heap",
                    width: 7,
                    right: true,
                },
                Col {
                    header: "Shallow",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Object ID",
                    width: 18,
                    right: false,
                },
                Col {
                    header: "Class",
                    width: 40,
                    right: false,
                },
            ],
            &out.top_retained_objects
                .iter()
                .map(|e| {
                    vec![
                        fmt_size(e.retained_bytes),
                        fmt_pct(e.retained_bytes, out.total_shallow_bytes),
                        fmt_size_u32(e.shallow_bytes),
                        format!("0x{:016x}", e.object_id),
                        e.class_name.clone(),
                    ]
                })
                .collect::<Vec<_>>(),
        );
    }

    // ── Leak suspects ─────────────────────────────────────────────────────────
    if config.leak_suspects {
        // Use the pre-computed list from collect_output, which was built from the
        // full retained-by-class data before it was truncated to TOP_N. Re-filtering
        // the truncated `retained_by_class` here would miss suspects outside the top 20.
        if out.leak_suspects.is_empty() {
            println!();
            println!("── Leak suspects ──");
            println!();
            println!("  No single class retains ≥ 1% of the heap. No obvious suspects.");
        } else {
            println!();
            println!("── Leak suspects (classes retaining ≥ 1% of heap) ──");
            println!();
            for e in &out.leak_suspects {
                println!(
                    "  {:<50}  {}  ({} of heap)",
                    e.class_name,
                    fmt_size(e.total_retained_bytes),
                    fmt_pct(e.total_retained_bytes, out.total_shallow_bytes).trim(),
                );
                println!(
                    "    {} instances · {} avg retained · {}",
                    e.instance_count,
                    fmt_size(e.avg_retained_bytes),
                    e.pattern,
                );
                println!();
            }
        }
    }

    // ── Package summary ───────────────────────────────────────────────────────
    if config.package_summary {
        println!();
        println!("── Top {TOP_N} packages by retained heap ──");
        println!();
        print_table(
            &[
                Col {
                    header: "Retained",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "% heap",
                    width: 7,
                    right: true,
                },
                Col {
                    header: "Shallow",
                    width: 12,
                    right: true,
                },
                Col {
                    header: "Classes",
                    width: 7,
                    right: true,
                },
                Col {
                    header: "Instances",
                    width: 9,
                    right: true,
                },
                Col {
                    header: "Package",
                    width: 40,
                    right: false,
                },
            ],
            &out.package_summary
                .iter()
                .map(|e| {
                    vec![
                        fmt_size(e.total_retained_bytes),
                        fmt_pct(e.total_retained_bytes, out.total_shallow_bytes),
                        fmt_size(e.total_shallow_bytes),
                        e.class_count.to_string(),
                        e.instance_count.to_string(),
                        e.package.clone(),
                    ]
                })
                .collect::<Vec<_>>(),
        );
    }
}

/// Classify a suspect class into a human-readable pattern label.
fn classify_suspect(instance_count: u64, avg_retained: u64, total_retained: u64) -> &'static str {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    if instance_count == 1 {
        "singleton retaining a large subgraph"
    } else if instance_count >= 100 && avg_retained >= 10 * KB {
        "accumulator — many objects each holding significant state"
    } else if instance_count <= 5 && total_retained >= 10 * MB {
        "few instances dominating a large portion of the heap"
    } else {
        "elevated retention"
    }
}

// ── JSON renderer ─────────────────────────────────────────────────────────────

fn emit_json(out: &AnalysisOutput, config: &ReportConfig) {
    println!("{{");
    println!("  \"summary\": {{");
    println!("    \"total_objects\": {},", out.total_objects);
    println!("    \"total_classes\": {},", out.total_classes);
    println!("    \"gc_roots\": {},", out.gc_roots);
    println!("    \"total_shallow_bytes\": {},", out.total_shallow_bytes);
    println!("    \"retained_heap_bytes\": {},", out.retained_heap_bytes);
    println!("    \"unreachable_count\": {},", out.unreachable_count);
    println!(
        "    \"unreachable_shallow_bytes\": {}",
        out.unreachable_shallow
    );
    println!("  }},");

    let mut sections: Vec<String> = Vec::new();

    if config.histogram {
        let render = |entries: &[ClassHistEntry]| -> String {
            let rows: Vec<String> = entries.iter().map(|e| format!(
                "    {{\"class_name\":{},\"instances\":{},\"total_shallow_bytes\":{},\"max_shallow_bytes\":{}}}",
                json_str(&e.class_name), e.instances, e.total_shallow_bytes, e.max_shallow_bytes,
            )).collect();
            format!("[\n{}\n  ]", rows.join(",\n"))
        };
        sections.push(format!(
            "  \"top_allocated_classes\": {}",
            render(&out.top_allocated)
        ));
        sections.push(format!(
            "  \"top_largest_instances\": {}",
            render(&out.top_largest)
        ));
    }

    if config.retained_by_class {
        let rows: Vec<String> = out.retained_by_class.iter().map(|e| {
            let avg = if e.instance_count > 0 { e.total_retained_bytes / e.instance_count } else { 0 };
            format!(
                "    {{\"class_name\":{},\"instance_count\":{},\"total_retained_bytes\":{},\"total_shallow_bytes\":{},\"avg_retained_bytes\":{}}}",
                json_str(&e.class_name), e.instance_count, e.total_retained_bytes, e.total_shallow_bytes, avg,
            )
        }).collect();
        sections.push(format!(
            "  \"retained_by_class\": [\n{}\n  ]",
            rows.join(",\n")
        ));

        let obj_rows: Vec<String> = out.top_retained_objects.iter().map(|e| format!(
            "    {{\"object_id\":\"0x{:016x}\",\"class_name\":{},\"shallow_bytes\":{},\"retained_bytes\":{}}}",
            e.object_id, json_str(&e.class_name), e.shallow_bytes, e.retained_bytes,
        )).collect();
        sections.push(format!(
            "  \"top_retained_objects\": [\n{}\n  ]",
            obj_rows.join(",\n")
        ));
    }

    if config.leak_suspects {
        // Use the pre-computed list (built from the full class data, not the
        // truncated top-20 retained_by_class slice).
        let suspect_rows: Vec<String> = out.leak_suspects.iter().map(|e| {
            format!(
                "    {{\"class_name\":{},\"instance_count\":{},\"total_retained_bytes\":{},\"avg_retained_bytes\":{},\"pct_of_heap\":{:.2},\"pattern\":{}}}",
                json_str(&e.class_name), e.instance_count, e.total_retained_bytes,
                e.avg_retained_bytes, e.pct_of_heap,
                json_str(e.pattern),
            )
        }).collect();
        sections.push(format!(
            "  \"leak_suspects\": [\n{}\n  ]",
            suspect_rows.join(",\n")
        ));
    }

    if config.package_summary {
        let rows: Vec<String> = out.package_summary.iter().map(|e| format!(
            "    {{\"package\":{},\"class_count\":{},\"instance_count\":{},\"total_shallow_bytes\":{},\"total_retained_bytes\":{}}}",
            json_str(&e.package), e.class_count, e.instance_count, e.total_shallow_bytes, e.total_retained_bytes,
        )).collect();
        sections.push(format!(
            "  \"package_summary\": [\n{}\n  ]",
            rows.join(",\n")
        ));
    }

    println!("{}", sections.join(",\n"));
    println!("}}");
}

// ── Path to GC root ───────────────────────────────────────────────────────────

fn lookup_object(file: &mut File, entry_count: u64, target_id: u64) -> Result<Option<(u64, u32)>> {
    let mut lo = 0u64;
    let mut hi = entry_count;
    let mut buf = [0u8; ENTRY_SIZE];
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        file.seek(SeekFrom::Start(mid * ENTRY_SIZE as u64))
            .context("seek object index")?;
        file.read_exact(&mut buf)
            .context("read object index entry")?;
        let oid = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        match oid.cmp(&target_id) {
            std::cmp::Ordering::Equal => {
                let cid = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                let sz = u32::from_le_bytes(buf[16..20].try_into().unwrap());
                return Ok(Some((cid, sz)));
            }
            std::cmp::Ordering::Less => lo = mid + 1,
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
        file.seek(SeekFrom::Start(mid * EDGE_SIZE as u64))
            .context("seek reverse edges")?;
        file.read_exact(&mut buf)
            .context("read reverse edge entry")?;
        let to_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        if to_id < target_id {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let mut referrers = Vec::new();
    let mut pos = lo;
    file.seek(SeekFrom::Start(pos * EDGE_SIZE as u64))
        .context("seek reverse edges to lower bound")?;
    loop {
        if pos >= entry_count {
            break;
        }
        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e).context("read reverse edge"),
        }
        let to_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let from_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if to_id != target_id {
            break;
        }
        referrers.push(from_id);
        pos += 1;
    }
    Ok(referrers)
}

pub fn path_to_root(
    target_id: u64,
    pass1: &Pass1Output,
    pass2: &Pass2Output,
    json: bool,
) -> Result<()> {
    let roots: HashSet<u64> = pass1.roots.iter().copied().collect();

    let obj_entry_count = std::fs::metadata(&pass1.object_index_path)?.len() / ENTRY_SIZE as u64;
    let mut obj_file =
        File::open(&pass1.object_index_path).context("open object index for path query")?;
    let rev_entry_count = std::fs::metadata(&pass2.reverse_edges_path)?.len() / EDGE_SIZE as u64;
    let mut rev_file =
        File::open(&pass2.reverse_edges_path).context("open reverse edges for path query")?;

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

    let mut prev: HashMap<u64, u64> = HashMap::new();
    prev.insert(target_id, u64::MAX);
    let mut queue: VecDeque<u64> = VecDeque::new();
    queue.push_back(target_id);

    let found_root = if roots.contains(&target_id) {
        Some(target_id)
    } else {
        let mut found = None;
        'bfs: while let Some(obj) = queue.pop_front() {
            for referrer in find_referrers(&mut rev_file, rev_entry_count, obj)? {
                if prev.contains_key(&referrer) {
                    continue;
                }
                prev.insert(referrer, obj);
                if roots.contains(&referrer) {
                    found = Some(referrer);
                    break 'bfs;
                }
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

    let mut path_ids = vec![root];
    let mut cur = root;
    while cur != target_id {
        cur = *prev.get(&cur).unwrap();
        path_ids.push(cur);
    }

    let mut steps: Vec<PathStep> = Vec::with_capacity(path_ids.len());
    for &oid in &path_ids {
        let (cid, sz) = lookup_object(&mut obj_file, obj_entry_count, oid)?.unwrap_or((0, 0));
        steps.push(PathStep {
            object_id: oid,
            class_name: class_name(cid, &pass1.class_index),
            shallow_bytes: sz,
            is_gc_root: oid == root,
            is_target: oid == target_id,
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
            let label = if s.is_gc_root && s.is_target {
                " ← GC root + target"
            } else if s.is_gc_root {
                " ← GC root"
            } else if s.is_target {
                " ← target"
            } else {
                ""
            };
            let prefix = if i == 0 {
                "  ".to_string()
            } else {
                "  → ".to_string()
            };
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

// ── Public entry points ───────────────────────────────────────────────────────

/// Run the selected analyses and emit results to stdout.
/// Progress messages go to stderr regardless of mode.
pub fn run(
    pass1: &Pass1Output,
    pass4: &Pass4Output,
    _output_dir: &Path,
    json: bool,
    config: &ReportConfig,
) -> Result<()> {
    let obj_idx = ObjectIndex::open(&pass1.object_index_path)?;
    let out = collect_output(&obj_idx, &pass4.retained_path, &pass1.class_index, pass1, pass4)?;
    if json {
        emit_json(&out, config);
    } else {
        emit_text(&out, config);
    }
    Ok(())
}

/// Generate a self-contained HTML report and write it to `html_path`.
pub fn run_html(pass1: &Pass1Output, pass4: &Pass4Output, html_path: &Path) -> Result<()> {
    let obj_idx = ObjectIndex::open(&pass1.object_index_path)?;
    let out = collect_output(&obj_idx, &pass4.retained_path, &pass1.class_index, pass1, pass4)?;
    let html_str = html::render(&out);
    std::fs::write(html_path, html_str.as_bytes())
        .with_context(|| format!("write HTML report to {}", html_path.display()))?;
    Ok(())
}
