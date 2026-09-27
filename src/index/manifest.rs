//! Completion manifest for immutable index files.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const REQUIRED_FILES: &[&str] = &[
    "object_index.bin",
    "class_names.bin",
    "retained.bin",
    "meta.bin",
    "edges.bin",
    "roots.bin",
    "shallow_sizes.bin",
    "idom.bin",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexManifest {
    pub version: u32,
    pub generation: String,
    pub files: BTreeMap<String, u64>,
}

pub fn read(dir: &Path) -> Result<IndexManifest> {
    let path = dir.join("manifest.json");
    let manifest: IndexManifest = serde_json::from_reader(
        std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    anyhow::ensure!(manifest.version == 1, "unsupported index manifest version");
    anyhow::ensure!(!manifest.generation.is_empty(), "empty index generation");
    for name in REQUIRED_FILES {
        let expected = manifest
            .files
            .get(*name)
            .with_context(|| format!("manifest missing {name}"))?;
        let actual = std::fs::metadata(dir.join(name))
            .with_context(|| format!("stat index file {name}"))?
            .len();
        anyhow::ensure!(
            actual == *expected,
            "index file {name} size mismatch: expected {expected}, got {actual}"
        );
    }
    Ok(manifest)
}

pub fn publish(dir: &Path) -> Result<IndexManifest> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut files = BTreeMap::new();
    for name in REQUIRED_FILES {
        let len = std::fs::metadata(dir.join(name))
            .with_context(|| format!("stat index file {name}"))?
            .len();
        files.insert((*name).to_string(), len);
    }
    let generation = format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let manifest = IndexManifest {
        version: 1,
        generation,
        files,
    };
    let path = dir.join("manifest.json");
    let tmp = dir.join(format!("manifest.json.{}.tmp", std::process::id()));
    let mut f = std::fs::File::create(&tmp).context("create manifest temporary file")?;
    serde_json::to_writer(&mut f, &manifest).context("serialize index manifest")?;
    f.flush().context("flush index manifest")?;
    f.sync_all().context("sync index manifest")?;
    std::fs::rename(&tmp, &path).context("publish index manifest")?;
    Ok(manifest)
}

pub fn generation(dir: &Path) -> Result<String> {
    Ok(read(dir)?.generation)
}
