//! Per-pass benchmarks for minprof.
//!
//! Each benchmark times a single pass in isolation. Passes prior to the one
//! under test are pre-run once into `target/bench-fixtures/` and reused across
//! iterations so only the target pass is measured.
//!
//! # Running
//!
//!   cargo bench
//!   cargo bench -- pass1          # filter to pass1 benchmarks only
//!   cargo bench -- --save-baseline before
//!   cargo bench -- --baseline before   # compare against saved baseline
//!
//! # Fixture scale
//!
//! Controlled by BENCH_OBJECTS / BENCH_CLASSES / BENCH_ROOTS below.
//! Adjust to trade off benchmark duration vs sensitivity.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

use minprof::passes;
use minprof::passes::dominators::Pass3Output;
use minprof::passes::edges::Pass2Output;
use minprof::passes::index::{Pass1Output, load_class_index, load_roots};

// ── Fixture parameters ────────────────────────────────────────────────────────

const BENCH_OBJECTS: &str = "1000000";
const BENCH_CLASSES: &str = "1000";
const BENCH_ROOTS: &str = "5000";

const GEN_HPROF: &str = env!("CARGO_BIN_EXE_gen_hprof");

// ── Fixture ───────────────────────────────────────────────────────────────────

struct Fixture {
    hprof: PathBuf,
    dir: PathBuf,
    object_count: u64,
    edge_count: u64,
    node_count: u32,
}

static FIXTURE: OnceLock<Fixture> = OnceLock::new();

fn fixture() -> &'static Fixture {
    FIXTURE.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bench-fixtures");
        fs::create_dir_all(&dir).unwrap();

        let hprof = dir.join("bench.hprof");
        // Sentinel: all pre-computed files are present.
        let sentinel = dir.join(".complete");

        if !sentinel.exists() {
            // (Re-)generate from scratch.
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();

            eprintln!(
                "[bench] generating fixture: {} objects, {} classes, {} roots",
                BENCH_OBJECTS, BENCH_CLASSES, BENCH_ROOTS,
            );
            let status = std::process::Command::new(GEN_HPROF)
                .args([
                    "--output",
                    hprof.to_str().unwrap(),
                    "--objects",
                    BENCH_OBJECTS,
                    "--classes",
                    BENCH_CLASSES,
                    "--roots",
                    BENCH_ROOTS,
                ])
                .status()
                .expect("gen_hprof binary not found — run `cargo build --release` first");
            assert!(status.success(), "gen_hprof exited with {status}");

            eprintln!("[bench] running pass 1…");
            let p1 = passes::index::run(&hprof, &dir).unwrap();
            eprintln!("[bench] running pass 2…");
            let p2 = passes::edges::run(&hprof, &p1, &dir).unwrap();
            eprintln!("[bench] running pass 3…");
            let p3 = passes::dominators::run(&p1, &p2, &dir).unwrap();
            eprintln!("[bench] running pass 4…");
            passes::retained::run(&p1, &p3, &dir).unwrap();

            // Persist rpo_to_node so pass-4 benchmarks can reconstruct Pass3Output.
            write_u32_vec(&dir.join("rpo_to_node.bin"), &p3.rpo_to_node);

            let counts = format!("{} {} {}", p1.object_count, p2.edge_count, p3.node_count);
            fs::write(&sentinel, counts).unwrap();

            eprintln!("[bench] fixture ready.");
        }

        let counts_str = fs::read_to_string(&sentinel).unwrap();
        let mut parts = counts_str.split_whitespace();
        let object_count: u64 = parts.next().unwrap().parse().unwrap();
        let edge_count: u64 = parts.next().unwrap().parse().unwrap();
        let node_count: u32 = parts.next().unwrap().parse().unwrap();

        eprintln!(
            "[bench] fixture: {} objects, {} edges",
            object_count, edge_count
        );

        Fixture {
            hprof,
            dir,
            object_count,
            edge_count,
            node_count,
        }
    })
}

// ── Fixture helpers ───────────────────────────────────────────────────────────

fn load_pass1(f: &Fixture) -> Pass1Output {
    Pass1Output {
        class_index: load_class_index(&f.dir.join("class_names.bin")).unwrap(),
        roots: load_roots(&f.dir.join("roots.bin")).unwrap(),
        object_count: f.object_count,
        object_index_path: f.dir.join("object_index.bin"),
        shallow_sizes_path: f.dir.join("shallow_sizes.bin"),
    }
}

fn load_pass2(f: &Fixture) -> Pass2Output {
    Pass2Output {
        edges_path: f.dir.join("edges.bin"),
        edge_count: f.edge_count,
    }
}

fn load_pass3(f: &Fixture) -> Pass3Output {
    Pass3Output {
        idom_path: f.dir.join("idom.bin"),
        rpo_to_node: read_u32_vec(&f.dir.join("rpo_to_node.bin")),
        node_count: f.node_count,
    }
}

fn write_u32_vec(path: &Path, v: &[u32]) {
    let mut f = fs::File::create(path).unwrap();
    for &x in v {
        f.write_all(&x.to_le_bytes()).unwrap();
    }
}

fn read_u32_vec(path: &Path) -> Vec<u32> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

/// Create a unique temporary output directory for one benchmark iteration.
/// Cleaned up on drop via the returned handle.
struct TempOutDir(PathBuf);

impl TempOutDir {
    fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("minprof_bench_{label}_{n}"));
        fs::create_dir_all(&dir).unwrap();
        TempOutDir(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempOutDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// ── Pass 1 benchmark ──────────────────────────────────────────────────────────

fn bench_pass1(c: &mut Criterion) {
    let f = fixture();
    let mut group = c.benchmark_group("pass1_index");
    group.sample_size(10);

    group.bench_function(format!("{}_objects", f.object_count), |b| {
        b.iter_batched(
            || TempOutDir::new("p1"),
            |out| passes::index::run(&f.hprof, out.path()).unwrap(),
            BatchSize::PerIteration,
        )
    });

    group.finish();
}

// ── Pass 2 benchmark ──────────────────────────────────────────────────────────

fn bench_pass2(c: &mut Criterion) {
    let f = fixture();
    let mut group = c.benchmark_group("pass2_edges");
    group.sample_size(10);

    group.bench_function(format!("{}_objects", f.object_count), |b| {
        b.iter_batched(
            || (TempOutDir::new("p2"), load_pass1(f)),
            |(out, p1)| passes::edges::run(&f.hprof, &p1, out.path()).unwrap(),
            BatchSize::PerIteration,
        )
    });

    group.finish();
}

// ── Pass 3 benchmark ──────────────────────────────────────────────────────────

fn bench_pass3(c: &mut Criterion) {
    let f = fixture();
    let mut group = c.benchmark_group("pass3_dominators");
    group.sample_size(10);

    group.bench_function(format!("{}_objects", f.object_count), |b| {
        b.iter_batched(
            || (TempOutDir::new("p3"), load_pass1(f), load_pass2(f)),
            |(out, p1, p2)| passes::dominators::run(&p1, &p2, out.path()).unwrap(),
            BatchSize::PerIteration,
        )
    });

    group.finish();
}

// ── Pass 4 benchmark ──────────────────────────────────────────────────────────

fn bench_pass4(c: &mut Criterion) {
    let f = fixture();
    let mut group = c.benchmark_group("pass4_retained");
    group.sample_size(10);

    group.bench_function(format!("{}_objects", f.object_count), |b| {
        b.iter_batched(
            || (TempOutDir::new("p4"), load_pass1(f), load_pass3(f)),
            |(out, p1, p3)| passes::retained::run(&p1, &p3, out.path()).unwrap(),
            BatchSize::PerIteration,
        )
    });

    group.finish();
}

// ── Registration ──────────────────────────────────────────────────────────────

criterion_group!(benches, bench_pass1, bench_pass2, bench_pass3, bench_pass4);
criterion_main!(benches);
