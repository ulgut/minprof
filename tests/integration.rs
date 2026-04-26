//! Integration tests: run the minprof binary against the small test fixtures and
//! compare every output file against the golden snapshots in `tests/.expected/`.
//!
//! To regenerate the golden snapshots:
//!
//!   cargo build
//!   mkdir -p tests/.expected/hprof-32 tests/.expected/hprof-64
//!   target/debug/minprof -p tests/hprof-32.bin -o tests/.expected/hprof-32 \
//!       > tests/.expected/hprof-32/stdout.txt
//!   target/debug/minprof -p tests/hprof-64.bin -o tests/.expected/hprof-64 \
//!       > tests/.expected/hprof-64/stdout.txt

use std::path::{Path, PathBuf};
use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_minprof");

/// Files produced by a full run that we compare against the golden snapshots.
/// `stdout.txt` is written by the test harness from the captured stdout.
const COMPARED_FILES: &[&str] = &[
    "stdout.txt",
    "meta.bin",
    "class_names.bin",
    "object_index.bin",
    "edges.bin",
    "roots.bin",
    "idom.bin",
    "retained.bin",
];

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Run minprof against `hprof_path`, write index files into `out_dir`,
/// and write captured stdout to `out_dir/stdout.txt`.
fn run_minprof(hprof_path: &Path, out_dir: &Path) {
    std::fs::create_dir_all(out_dir).expect("create test output dir");

    let output = Command::new(BINARY)
        .args([
            "-p",
            hprof_path.to_str().unwrap(),
            "-o",
            out_dir.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run minprof");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("minprof failed for {}: {}", hprof_path.display(), stderr);
    }

    std::fs::write(out_dir.join("stdout.txt"), &output.stdout).expect("write stdout.txt");
}

/// Run minprof via `-i` (index cache) and return the captured stdout.
fn run_minprof_index(index_dir: &Path) -> Vec<u8> {
    let output = Command::new(BINARY)
        .args(["-i", index_dir.to_str().unwrap()])
        .output()
        .expect("failed to run minprof -i");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("minprof -i failed for {}: {}", index_dir.display(), stderr);
    }

    output.stdout
}

/// Compare the test output directory against the golden snapshot directory.
///
/// Panics with a helpful diff-style message on the first mismatch.
fn compare_dirs(label: &str, test_dir: &Path, expected_dir: &Path) {
    for name in COMPARED_FILES {
        let test_path = test_dir.join(name);
        let expected_path = expected_dir.join(name);

        let test_bytes = match std::fs::read(&test_path) {
            Ok(b) => b,
            Err(e) => panic!("[{label}] missing output file {name}: {e}"),
        };
        let expected_bytes = match std::fs::read(&expected_path) {
            Ok(b) => b,
            Err(e) => panic!(
                "[{label}] missing golden file {name}: {e}\n\
                 Run the commands at the top of tests/integration.rs to regenerate."
            ),
        };

        if test_bytes != expected_bytes {
            if name.ends_with(".txt") {
                let got = String::from_utf8_lossy(&test_bytes);
                let exp = String::from_utf8_lossy(&expected_bytes);
                panic!(
                    "[{label}] {name} mismatch\n\
                     === expected ===\n{exp}\n\
                     === got ===\n{got}"
                );
            } else {
                panic!(
                    "[{label}] {name} mismatch: expected {} bytes, got {} bytes",
                    expected_bytes.len(),
                    test_bytes.len()
                );
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn hprof_32_full_run() {
    let root = workspace_root();
    let hprof = root.join("tests/hprof-32.bin");
    let test_dir = root.join("tests/.test/hprof-32");
    let expected = root.join("tests/.expected/hprof-32");

    // Clean previous test output so stale files don't mask regressions.
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).expect("clean test dir");
    }

    run_minprof(&hprof, &test_dir);
    compare_dirs("hprof-32", &test_dir, &expected);
}

#[test]
fn hprof_64_full_run() {
    let root = workspace_root();
    let hprof = root.join("tests/hprof-64.bin");
    let test_dir = root.join("tests/.test/hprof-64");
    let expected = root.join("tests/.expected/hprof-64");

    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).expect("clean test dir");
    }

    run_minprof(&hprof, &test_dir);
    compare_dirs("hprof-64", &test_dir, &expected);
}

/// Verify that re-running with `-i` (index cache) produces identical output
/// to the fresh `-p` run (i.e. no output depends on re-parsing the HPROF).
#[test]
fn hprof_64_index_cache_matches() {
    let root = workspace_root();
    let hprof = root.join("tests/hprof-64.bin");
    let test_dir = root.join("tests/.test/hprof-64-cache");
    let expected = root.join("tests/.expected/hprof-64");

    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).expect("clean test dir");
    }

    // First run: build the index.
    run_minprof(&hprof, &test_dir);

    // Second run: load from index only.
    let cached_stdout = run_minprof_index(&test_dir);

    // The report stdout must be identical whether parsed fresh or loaded.
    let expected_stdout = std::fs::read(expected.join("stdout.txt"))
        .expect("golden stdout.txt missing — run the commands at the top of tests/integration.rs");

    if cached_stdout != expected_stdout {
        let got = String::from_utf8_lossy(&cached_stdout);
        let exp = String::from_utf8_lossy(&expected_stdout);
        panic!(
            "[hprof-64 index cache] stdout mismatch\n\
             === expected ===\n{exp}\n\
             === got ===\n{got}"
        );
    }
}

#[test]
fn hprof_32_index_cache_matches() {
    let root = workspace_root();
    let hprof = root.join("tests/hprof-32.bin");
    let test_dir = root.join("tests/.test/hprof-32-cache");
    let expected = root.join("tests/.expected/hprof-32");

    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).expect("clean test dir");
    }

    run_minprof(&hprof, &test_dir);

    let cached_stdout = run_minprof_index(&test_dir);

    let expected_stdout = std::fs::read(expected.join("stdout.txt"))
        .expect("golden stdout.txt missing — run the commands at the top of tests/integration.rs");

    if cached_stdout != expected_stdout {
        let got = String::from_utf8_lossy(&cached_stdout);
        let exp = String::from_utf8_lossy(&expected_stdout);
        panic!(
            "[hprof-32 index cache] stdout mismatch\n\
             === expected ===\n{exp}\n\
             === got ===\n{got}"
        );
    }
}
