# minprof

A streaming, multi-pass JVM heap dump analyser. Eclipse MAT and VisualVM load the entire heap into RAM — impractical for dumps larger than your available memory. `minprof` streams `.hprof` files in multiple passes, keeping intermediate data on disk, so it can handle files many times larger than available RAM without sacrificing insight.

## Installation

```sh
cargo install --path .
```

## Usage

```
minprof [OPTIONS] <HPROF>

Arguments:
  <HPROF>  Path to the .hprof file

Options:
  -o, --output <DIR>       Directory for intermediate index files [default: <hprof>.minprof/]
      --format <FORMAT>    Output format [default: pretty]
      --report <REPORT>    Which analyses to run [default: all]
      --path <OBJECT_ID>   Print shortest reference path from a GC root to this object
  -h, --help               Print help
```

### `--format`

| Value    | Description |
|----------|-------------|
| `pretty` | Human-readable text tables (default) |
| `json`   | Newline-delimited JSON on stdout; progress on stderr |
| `html`   | Self-contained HTML report written to `<hprof>.html` |

### `--report`

Selects which analyses to emit. Repeatable or comma-separated. Default: `all`.

| Value       | Eclipse MAT equivalent          | Description |
|-------------|--------------------------------|-------------|
| `all`       | All reports                    | Run every analysis (default) |
| `histogram` | Class Histogram                | Object count + shallow bytes per class |
| `retained`  | Dominator Tree (by class)      | Retained heap grouped by class, top individual objects by retained size |
| `leaks`     | Leak Suspects                  | Classes retaining ≥ 1% of heap, with pattern classification |
| `packages`  | System Overview → Package view | Memory rollup by Java package |

### Examples

```sh
# Default: pretty text, all analyses
minprof heap.hprof

# HTML report (treemap + charts + leak suspects)
minprof heap.hprof --format html

# JSON, retained analysis only
minprof heap.hprof --format json --report retained

# Leak suspects + package summary, text output
minprof heap.hprof --report leaks,packages

# Path to GC root for an object ID from the retained table
minprof heap.hprof --path 0x00000000d6fc57f0
```

## Output

### Text (`--format pretty`)

Four sections, each gated by `--report`:

```
Total heap: 2.45MiB shallow across 8262 objects (474 classes, 429 GC roots).
Retained heap (reachable): 237.96KiB.
Unreachable (garbage): 3533 objects, 2.22MiB — not held by any GC root.

── Top 20 classes by total allocation ──
── Top 20 classes by largest single instance ──
── Top 20 classes by retained heap ──
── Top 20 individual objects by retained heap ──  ← object IDs for --path
── Leak suspects (classes retaining ≥ 1% of heap) ──
── Top 20 packages by retained heap ──
```

The "Top 20 individual objects by retained heap" table shows object IDs you can pass directly to `--path`:

```
  0x00000000d6fc57f0  sun.misc.Launcher$AppClassLoader  retained: 48.69KiB
```

### HTML (`--format html`)

Writes a single self-contained `.html` file with no external dependencies:

- **Heap Overview** — summary cards: total shallow, retained, classes, unreachable, finalizer queue depth, soft/weak/phantom reference counts
- **GC Pressure** — reference statistics and unreachable object breakdown (Eclipse MAT "System Overview" style)
- **Leak Suspects** — numbered "Problem Suspect" cards with pattern classification
- **Retained Heap Treemap** — interactive canvas treemap; click a package to drill into its classes
- **Class Histogram** — bar charts for top classes by allocation and retained heap
- **Package Summary** — table of all packages by retained heap

### JSON (`--format json`)

Progress goes to **stderr**; only results go to **stdout** — safe to pipe or redirect.

The analysis result is a single JSON object:

```json
{
  "summary": { "total_objects": 8262, "total_classes": 474, "gc_roots": 429,
               "total_shallow_bytes": 2568192, "retained_heap_bytes": 243670,
               "unreachable_count": 3533, "unreachable_shallow_bytes": 2324480 },
  "top_allocated_classes": [
    { "class_name": "int[]", "instances": 436, "total_shallow_bytes": 2088960, "max_shallow_bytes": 649998 }
  ],
  "top_largest_instances": [ ... ],
  "retained_by_class": [
    { "class_name": "int[]", "instance_count": 436, "total_retained_bytes": 2088960,
      "total_shallow_bytes": 2088960, "avg_retained_bytes": 4793 }
  ],
  "top_retained_objects": [
    { "object_id": "0x00000000d701ec08", "class_name": "int[]",
      "shallow_bytes": 649998, "retained_bytes": 649998 }
  ],
  "leak_suspects": [
    { "class_name": "int[]", "instance_count": 436, "total_retained_bytes": 2088960,
      "avg_retained_bytes": 4793, "pct_of_heap": 81.4, "pattern": "elevated retention" }
  ],
  "package_summary": [
    { "package": "<primitive arrays>", "class_count": 5, "instance_count": 2891,
      "total_shallow_bytes": 2380800, "total_retained_bytes": 2380800 }
  ]
}
```

When `--path` is also given, a second JSON object follows on the next line ([NDJSON](https://ndjson.org/)):

```json
{"type":"path_to_root","target_id":"0x00000000d6fc57f0","found":true,"path":[
  {"object_id":"0x00000001234ab000","class_name":"java.lang.Thread","shallow_bytes":96,"is_gc_root":true,"is_target":false},
  {"object_id":"0x00000000d6fc57f0","class_name":"sun.misc.Launcher$AppClassLoader","shallow_bytes":130,"is_gc_root":false,"is_target":true}
]}
```

When the object is not found or is unreachable, `found` is `false` and an `error` key explains why.

## Memory usage

| Pass | What it does | Peak extra RAM |
|------|-------------|----------------|
| Pass 1 — index | Parse HPROF, build class index, write `object_index.bin` | ~100 MB (class index) |
| Pass 2 — edges | Extract object references, sort into `edges.bin` + `reverse_edges.bin` | 256 MB sort buffer |
| Pass 3 — dominator tree | CHK iterative algorithm on in-memory CSR graph | proportional to edge count |
| Pass 4 — retained sizes | Bottom-up dominator tree walk | O(N) |

The class index (kept in RAM across all passes) is typically under 100 MB for real-world JVM applications regardless of heap size.

## Comparison

|                         | minprof       | Eclipse MAT       | VisualVM          |
|-------------------------|---------------|-------------------|-------------------|
| Peak RAM                | ~1 GB         | ≈ dump size       | ≈ dump size       |
| 100 GB dump             | works         | needs ~100 GB RAM | needs ~100 GB RAM |
| Retained heap           | ✅            | ✅                 | ✅                 |
| Dominator tree          | ✅            | ✅                 | ❌                 |
| Path to GC root         | ✅            | ✅                 | ❌                 |
| Leak suspects           | ✅            | ✅                 | ❌                 |
| GC pressure metrics     | ✅            | ✅                 | partial           |
| Interactive treemap     | ✅ (HTML)     | ✅                 | ❌                 |
| JSON / scriptable       | ✅            | ❌                 | ❌                 |

## Limitations

- CLI only — no interactive query shell
- Tested on 64-bit HotSpot HPROF format (`id_size = 8`); 32-bit dumps (`id_size = 4`) parse correctly but are less tested
- Hobby project — use with caution

---

## References

- [hprof-slurp](https://github.com/agourlay/hprof-slurp) — single-pass streaming HPROF parser in Rust by Arnaud Gourlay. minprof's parser layer is adapted from this project (Apache 2.0).
- [HPROF format specification](https://docs.oracle.com/javase/8/docs/technotes/samples/hprof.html)
- [Cooper, Harvey, Kennedy — "A Simple, Fast Dominance Algorithm"](https://www.cs.tufts.edu/~nr/cs257/archive/keith-cooper/dom14.pdf) — the CHK iterative dominator algorithm used in pass 3
