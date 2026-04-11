mod anon;
mod index;
mod parser;
mod passes;
mod query;

use std::path::PathBuf;
use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(name = "minprof", about = "Streaming, multi-pass HPROF heap dump analyser")]
struct Cli {
    /// Path to the .hprof file
    hprof: PathBuf,

    /// Directory for intermediate index files (default: <hprof>.minprof/)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Print the shortest reference path from a GC root to this object.
    /// Provide the object ID as a hex address (e.g. 0x7f3a1c or 7f3a1c).
    #[arg(long, value_name = "OBJECT_ID")]
    path: Option<String>,

    /// Emit results as JSON instead of formatted text.
    /// Progress messages still go to stderr; only results go to stdout.
    /// Each result is a self-contained JSON object (NDJSON when combined with --path).
    #[arg(long)]
    json: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let output_dir = cli.output.unwrap_or_else(|| {
        let mut p = cli.hprof.clone();
        p.set_extension("minprof");
        p
    });

    eprintln!("output dir: {}", output_dir.display());

    eprintln!("=== pass 1: build index ===");
    let pass1 = passes::index::run(&cli.hprof, &output_dir)?;
    eprintln!(
        "  {} objects, {} classes, {} roots",
        pass1.object_count,
        pass1.class_index.len(),
        pass1.roots.len(),
    );

    eprintln!("=== pass 2: extract edges ===");
    let pass2 = passes::edges::run(&cli.hprof, &pass1, &output_dir)?;
    eprintln!("  {} references", pass2.edge_count);

    eprintln!("=== pass 3: dominator tree ===");
    let pass3 = passes::dominators::run(&pass1, &pass2, &output_dir)?;

    eprintln!("=== pass 4: retained sizes ===");
    let pass4 = passes::retained::run(&pass1, &pass3, &output_dir)?;

    eprintln!(
        "done — {:.2} MiB retained heap across {} objects",
        pass4.total_heap_bytes as f64 / 1_048_576.0,
        pass4.node_count,
    );

    eprintln!("=== query ===");
    query::run(&pass1, &pass4, &output_dir, cli.json)?;

    if let Some(raw_id) = cli.path {
        let target_id = parse_hex_id(&raw_id)?;
        query::path_to_root(target_id, &pass1, &pass2, cli.json)?;
    }

    Ok(())
}

fn parse_hex_id(s: &str) -> anyhow::Result<u64> {
    let hex = s.trim_start_matches("0x").trim_start_matches("0X");
    u64::from_str_radix(hex, 16)
        .map_err(|_| anyhow::anyhow!("invalid object ID '{}': expected a hex address (e.g. 0x7f3a1c)", s))
}
