mod index;
mod parser;
mod passes;
mod query;

use std::path::PathBuf;
use anyhow::Result;
use clap::Parser;

use query::ReportConfig;

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
                  Progress messages go to stderr; results go to stdout."
)]
struct Cli {
    /// Path to the .hprof file
    hprof: PathBuf,

    /// Directory for intermediate index files (default: <hprof>.minprof/)
    #[arg(short, long, value_name = "DIR")]
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
        histogram:         reports.contains(&Report::Histogram),
        retained_by_class: reports.contains(&Report::Retained),
        leak_suspects:     reports.contains(&Report::Leaks),
        package_summary:   reports.contains(&Report::Packages),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let output_dir = cli.output.unwrap_or_else(|| {
        let mut p = cli.hprof.clone();
        p.set_extension("minprof");
        p
    });

    eprintln!("output dir: {}", output_dir.display());

    let total_start = std::time::Instant::now();

    eprintln!("=== pass 1: build index ===");
    let t = std::time::Instant::now();
    let pass1 = passes::index::run(&cli.hprof, &output_dir)?;
    eprintln!("  {} objects, {} classes, {} roots  [{:.1}s]",
        pass1.object_count, pass1.class_index.len(), pass1.roots.len(),
        t.elapsed().as_secs_f64());

    eprintln!("=== pass 2: extract edges ===");
    let t = std::time::Instant::now();
    let pass2 = passes::edges::run(&cli.hprof, &pass1, &output_dir)?;
    eprintln!("  {} references  [{:.1}s]", pass2.edge_count, t.elapsed().as_secs_f64());

    eprintln!("=== pass 3: dominator tree ===");
    let t = std::time::Instant::now();
    let pass3 = passes::dominators::run(&pass1, &pass2, &output_dir)?;
    eprintln!("  [{:.1}s]", t.elapsed().as_secs_f64());

    eprintln!("=== pass 4: retained sizes ===");
    let t = std::time::Instant::now();
    let pass4 = passes::retained::run(&pass1, &pass3, &output_dir)?;
    eprintln!("  {:.2} MiB retained across {} objects  [{:.1}s]",
        pass4.total_heap_bytes as f64 / 1_048_576.0, pass4.node_count,
        t.elapsed().as_secs_f64());

    let config = build_report_config(&cli.report);
    let json   = cli.format == Format::Json;

    eprintln!("=== query ===");
    let t = std::time::Instant::now();
    match cli.format {
        Format::Html => {
            let html_path = cli.hprof.with_extension("html");
            query::run_html(&pass1, &pass4, &html_path)?;
            eprintln!("  wrote {}  [{:.1}s]", html_path.display(), t.elapsed().as_secs_f64());
        }
        _ => {
            query::run(&pass1, &pass4, &output_dir, json, &config)?;
            eprintln!("  [{:.1}s]", t.elapsed().as_secs_f64());
        }
    }

    if let Some(raw_id) = cli.path {
        let target_id = parse_hex_id(&raw_id)?;
        query::path_to_root(target_id, &pass1, &pass2, json)?;
    }

    eprintln!("=== done in {:.1}s total ===", total_start.elapsed().as_secs_f64());

    Ok(())
}

fn parse_hex_id(s: &str) -> anyhow::Result<u64> {
    let hex = s.trim_start_matches("0x").trim_start_matches("0X");
    u64::from_str_radix(hex, 16)
        .map_err(|_| anyhow::anyhow!("invalid object ID '{}': expected a hex address (e.g. 0x7f3a1c)", s))
}
