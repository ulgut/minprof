use anyhow::{Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};

use minprof::passes;
use minprof::passes::edges::{EDGE_SIZE, Pass2Output};
use minprof::passes::index::{ENTRY_SIZE, Pass1Output, load_class_index, load_roots};
use minprof::passes::retained::Pass4Output;
use minprof::query;
use minprof::query::ReportConfig;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq, Eq)]
enum Format {
    /// Human-readable text tables (default)
    Pretty,
    /// Newline-delimited JSON — stdout only, progress on stderr
    Json,
    /// Self-contained HTML report written to <hprof>.html
    Html,
}

/// Which analyses to include. Repeatable or comma-separated.
#[derive(clap::ValueEnum, Clone, Debug, PartialEq, Eq)]
enum Report {
    /// All analyses (default)
    All,
    /// Class histogram: object count + shallow bytes per class
    Histogram,
    /// Retained heap grouped by class (dominator tree view)
    Retained,
    /// Leak suspects: classes retaining ≥1% of heap
    Leaks,
    /// Package-level memory rollup
    Packages,
}

#[derive(Parser)]
#[command(
    name = "minprof",
    about = "Streaming HPROF heap-dump analyser",
    long_about = "Multi-pass streaming analyser for JVM heap dumps (.hprof).\n\
                  Processes files larger than available RAM via on-disk index files.\n\
                  Progress messages go to stderr; results go to stdout.\n\n\
                  First run: -p heap.hprof [-o /path/to/index/]\n\
                  Re-run reports: -i /path/to/index/ [--format html] [--report leaks]"
)]
#[command(group(
    clap::ArgGroup::new("source")
        .required(true)
        .args(["profile", "index_cache"]),
))]
struct Cli {
    /// Path to the .hprof file to parse and index.
    #[arg(short = 'p', long, value_name = "FILE")]
    profile: Option<PathBuf>,

    /// Path to an existing index directory built by a previous run.
    /// Skips all parse passes and generates reports immediately.
    #[arg(short = 'i', long, value_name = "DIR")]
    index_cache: Option<PathBuf>,

    /// Directory for index files and output reports.
    /// With -p, defaults to <hprof>.minprof/. With -i, defaults to the index dir.
    #[arg(short = 'o', long, value_name = "DIR")]
    output: Option<PathBuf>,

    /// Output format
    #[arg(long, value_enum, default_value = "pretty")]
    format: Format,

    /// Which analyses to run (repeatable or comma-separated)
    #[arg(long, value_enum, value_delimiter = ',', default_values_t = vec![Report::All])]
    report: Vec<Report>,

    /// Print the shortest reference path from a GC root to this object.
    /// Use an object ID from the retained-heap table (e.g. 0x7f3a1c80).
    #[arg(long, value_name = "OBJECT_ID")]
    path: Option<String>,
}

fn build_report_config(reports: &[Report]) -> ReportConfig {
    if reports.contains(&Report::All) {
        return ReportConfig::all();
    }
    ReportConfig {
        histogram: reports.contains(&Report::Histogram),
        retained_by_class: reports.contains(&Report::Retained),
        leak_suspects: reports.contains(&Report::Leaks),
        package_summary: reports.contains(&Report::Packages),
    }
}

// ── meta.bin ─────────────────────────────────────────────────────────────────
//
// 7 × u64 (little-endian):
//   [0]  version = 1
//   [1]  object_count
//   [2]  root_count
//   [3]  edge_count
//   [4]  total_heap_bytes
//   [5]  unreachable_count
//   [6]  unreachable_shallow_bytes

fn write_meta(
    path: &Path,
    object_count: u64,
    root_count: u64,
    edge_count: u64,
    total_heap_bytes: u64,
    unreachable_count: u64,
    unreachable_shallow: u64,
) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path).context("create meta.bin")?;
    for v in [
        1u64,
        object_count,
        root_count,
        edge_count,
        total_heap_bytes,
        unreachable_count,
        unreachable_shallow,
    ] {
        f.write_all(&v.to_le_bytes())?;
    }
    Ok(())
}

fn read_meta(path: &Path) -> Result<[u64; 7]> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).context("open meta.bin")?;
    let mut buf = [0u8; 56];
    f.read_exact(&mut buf).context("read meta.bin")?;
    let mut vals = [0u64; 7];
    for (i, v) in vals.iter_mut().enumerate() {
        *v = u64::from_le_bytes(buf[i * 8..(i + 1) * 8].try_into().unwrap());
    }
    Ok(vals)
}

// ── Index detection & loading ─────────────────────────────────────────────────

fn index_is_complete(dir: &Path) -> bool {
    [
        "object_index.bin",
        "class_names.bin",
        "retained.bin",
        "meta.bin",
        "edges.bin",
        "reverse_edges.bin",
    ]
    .iter()
    .all(|f| dir.join(f).exists())
}

fn load_index(dir: &Path) -> Result<(Pass1Output, Pass2Output, Pass4Output)> {
    let meta = read_meta(&dir.join("meta.bin"))?;
    // [version, object_count, root_count, edge_count,
    //  total_heap_bytes, unreachable_count, unreachable_shallow]

    let object_index_path = dir.join("object_index.bin");
    let object_count = std::fs::metadata(&object_index_path)
        .context("stat object_index.bin")?
        .len()
        / ENTRY_SIZE as u64;

    let pass1 = Pass1Output {
        class_index: load_class_index(&dir.join("class_names.bin"))?,
        roots: load_roots(&dir.join("roots.bin"))?,
        object_count,
        object_index_path,
        shallow_sizes_path: dir.join("shallow_sizes.bin"),
    };

    let edges_path = dir.join("edges.bin");
    let reverse_path = dir.join("reverse_edges.bin");
    let edge_count = std::fs::metadata(&edges_path)
        .context("stat edges.bin")?
        .len()
        / EDGE_SIZE as u64;

    let pass2 = Pass2Output {
        edges_path,
        reverse_edges_path: reverse_path,
        edge_count,
    };

    let pass4 = Pass4Output {
        retained_path: dir.join("retained.bin"),
        total_heap_bytes: meta[4],
        node_count: object_count as u32,
        unreachable_count: meta[5],
        unreachable_shallow: meta[6],
    };

    Ok((pass1, pass2, pass4))
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let cli = Cli::parse();

    let config = build_report_config(&cli.report);
    let is_json = cli.format == Format::Json;

    // ── Resume: load from existing index ─────────────────────────────────────
    if let Some(ref index_dir) = cli.index_cache {
        if !index_is_complete(index_dir) {
            anyhow::bail!(
                "index at '{}' is incomplete — missing one or more required .bin files",
                index_dir.display()
            );
        }
        eprintln!("index dir: {}", index_dir.display());
        eprintln!("Loading from existing index…");
        let (pass1, pass2, pass4) = load_index(index_dir).context("load index")?;
        eprintln!(
            "  {} objects, {} classes, {} roots",
            pass1.object_count,
            pass1.class_index.len(),
            pass1.roots.len()
        );
        run_query(&cli, &config, is_json, index_dir, &pass1, &pass2, &pass4)?;
        return Ok(());
    }

    // ── First run: parse the HPROF ────────────────────────────────────────────
    let hprof = cli.profile.as_deref().expect("clap group ensures -p or -i");

    let output_dir = cli.output.clone().unwrap_or_else(|| {
        let mut p = hprof.to_path_buf();
        p.set_extension("minprof");
        p
    });

    eprintln!("output dir: {}", output_dir.display());

    let total_start = std::time::Instant::now();

    eprintln!("=== pass 1: build index ===");
    let t = std::time::Instant::now();
    let pass1 = passes::index::run(hprof, &output_dir)?;
    eprintln!(
        "  {} objects, {} classes, {} roots  [{:.1}s]",
        pass1.object_count,
        pass1.class_index.len(),
        pass1.roots.len(),
        t.elapsed().as_secs_f64()
    );

    eprintln!("=== pass 2: extract edges ===");
    let t = std::time::Instant::now();
    let pass2 = passes::edges::run(hprof, &pass1, &output_dir)?;
    eprintln!(
        "  {} references  [{:.1}s]",
        pass2.edge_count,
        t.elapsed().as_secs_f64()
    );

    eprintln!("=== pass 3: dominator tree ===");
    let t = std::time::Instant::now();
    let pass3 = passes::dominators::run(&pass1, &pass2, &output_dir)?;
    eprintln!("  [{:.1}s]", t.elapsed().as_secs_f64());

    eprintln!("=== pass 4: retained sizes ===");
    let t = std::time::Instant::now();
    let pass4 = passes::retained::run(&pass1, &pass3, &output_dir)?;
    eprintln!(
        "  {:.2} MiB retained across {} objects  [{:.1}s]",
        pass4.total_heap_bytes as f64 / 1_048_576.0,
        pass4.node_count,
        t.elapsed().as_secs_f64()
    );

    write_meta(
        &output_dir.join("meta.bin"),
        pass1.object_count,
        pass1.roots.len() as u64,
        pass2.edge_count,
        pass4.total_heap_bytes,
        pass4.unreachable_count,
        pass4.unreachable_shallow,
    )
    .context("write meta.bin")?;

    eprintln!(
        "=== done in {:.1}s total ===",
        total_start.elapsed().as_secs_f64()
    );

    run_query(&cli, &config, is_json, &output_dir, &pass1, &pass2, &pass4)?;
    Ok(())
}

fn run_query(
    cli: &Cli,
    config: &ReportConfig,
    is_json: bool,
    output_dir: &Path,
    pass1: &Pass1Output,
    pass2: &Pass2Output,
    pass4: &Pass4Output,
) -> Result<()> {
    eprintln!("=== query ===");
    let t = std::time::Instant::now();

    match cli.format {
        Format::Html => {
            // HTML output: next to the profile file if available,
            // otherwise inside the output/index dir.
            let html_path = cli
                .profile
                .as_deref()
                .map(|p| p.with_extension("html"))
                .unwrap_or_else(|| output_dir.join("report.html"));
            query::run_html(pass1, pass4, &html_path)?;
            eprintln!(
                "  wrote {}  [{:.1}s]",
                html_path.display(),
                t.elapsed().as_secs_f64()
            );
        }
        _ => {
            query::run(pass1, pass4, output_dir, is_json, config)?;
            eprintln!("  [{:.1}s]", t.elapsed().as_secs_f64());
        }
    }

    if let Some(raw_id) = &cli.path {
        let target_id = parse_hex_id(raw_id)?;
        query::path_to_root(target_id, pass1, pass2, is_json)?;
    }

    Ok(())
}

fn parse_hex_id(s: &str) -> anyhow::Result<u64> {
    let hex = s.trim_start_matches("0x").trim_start_matches("0X");
    u64::from_str_radix(hex, 16).map_err(|_| {
        anyhow::anyhow!(
            "invalid object ID '{}': expected a hex address (e.g. 0x7f3a1c)",
            s
        )
    })
}
