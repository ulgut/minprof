pub mod dominators;
pub mod edges;
pub mod index;
pub mod retained;
pub mod sort;

use std::path::Path;

use anyhow::{Context, Result};

/// Buffer size for sequential reads/writes of multi-GB files (chunk flushes,
/// merge readers/writers, index loads).
pub const IO_BUF_SIZE: usize = 64 * 1024 * 1024;

pub const FALLBACK_MEM_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Maximum number of sorted chunk files merged in a single pass.
/// Above this, a two-level merge is used to cap peak file-descriptor count.
pub const MAX_MERGE_FAN_IN: usize = 64;

/// Returns total physical memory in bytes.
///
/// Reads `/proc/meminfo` on Linux; runs `sysctl -n hw.memsize` on macOS/BSD.
/// Falls back to 8 GiB if detection fails on any platform.
pub fn total_memory_bytes() -> u64 {
    // Linux
    if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                if let Ok(kb) = rest.split_whitespace().next().unwrap_or("").parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    // macOS / FreeBSD
    if let Ok(out) = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
    {
        if let Ok(s) = std::str::from_utf8(&out.stdout) {
            if let Ok(bytes) = s.trim().parse::<u64>() {
                return bytes;
            }
        }
    }
    FALLBACK_MEM_BYTES
}

/// Target sort-buffer size: 40% of physical RAM, clamped to [256 MiB, 128 GiB].
///
/// Both pass 1 and pass 2 run their external sort exclusively (no overlap),
/// so 40% is safe alongside the class-index and I/O-buffer overhead.
pub fn sort_chunk_bytes() -> usize {
    let mem = total_memory_bytes();
    let target = (mem as f64 * 0.40) as u64;
    target.clamp(256 * 1024 * 1024, 128 * 1024 * 1024 * 1024) as usize
}

/// Read a flat file of little-endian `u32` values into a `Vec<u32>`.
pub fn read_u32s(path: &Path) -> Result<Vec<u32>> {
    use std::io::BufRead;
    let file_len = std::fs::metadata(path)?.len() as usize;
    let mut data = Vec::with_capacity(file_len / 4);
    let mut reader = std::io::BufReader::with_capacity(
        IO_BUF_SIZE,
        std::fs::File::open(path).context("open file")?,
    );
    loop {
        let consumed = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let records = buf.len() / 4;
            for chunk in buf[..records * 4].chunks_exact(4) {
                data.push(u32::from_le_bytes(chunk.try_into().unwrap()));
            }
            records * 4
        };
        reader.consume(consumed);
    }
    Ok(data)
}
