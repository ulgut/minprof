pub mod dominators;
pub mod edges;
pub mod index;
pub mod retained;

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
    8 * 1024 * 1024 * 1024 // 8 GiB fallback
}

/// Target sort-buffer size: 40 % of physical RAM, clamped to [256 MiB, 32 GiB].
///
/// Both pass 1 and pass 2 run their external sort exclusively (no overlap),
/// so 40 % is safe alongside the class-index and I/O-buffer overhead.
pub fn sort_chunk_bytes() -> usize {
    let mem = total_memory_bytes();
    let target = (mem as f64 * 0.40) as u64;
    target.clamp(256 * 1024 * 1024, 32 * 1024 * 1024 * 1024) as usize
}
