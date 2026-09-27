// SPDX-License-Identifier: Apache-2.0
//
// Adapted from hprof-slurp <https://github.com/agourlay/hprof-slurp>
// Copyright (c) Arnaud Gourlay and hprof-slurp contributors.
// Licensed under the Apache License, Version 2.0.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result, anyhow};

use crate::parser::file_header_parser::{FileHeader, parse_file_header};

/// Stateful extractor for the HPROF reader. `finish` checks that
/// the final HPROF record and heap segment ended cleanly.
pub trait StreamExtractor<T>: Send + 'static {
    fn extract(&mut self, buf: &[u8], batch: &mut Vec<T>) -> usize;
    fn finish(&self) -> Result<()>;

    /// Pending payload bytes whose contents are not needed. `skip_bytes`
    /// validates their extent against the current heap segment.
    fn skippable_bytes(&self) -> usize {
        0
    }

    /// Account for bytes skipped by the input reader.
    fn skip_bytes(&mut self, bytes: usize) -> Result<()>;
}

/// HPROF reader. The extractor and reader share one thread so a seek
/// cannot race with read ahead. Primitive payloads are skipped only after
/// their headers have been parsed; batches use the normal extractor contract.
pub fn process_with_extractor<T, E>(
    path: &Path,
    mut extractor: E,
    on_batch: &mut dyn FnMut(&mut Vec<T>),
) -> Result<()>
where
    E: StreamExtractor<T>,
{
    const SEEK_READ_SIZE: usize = 1024 * 1024;
    let (_, header_bytes_consumed) = read_header(path)?;
    let mut file = File::open(path).context("open hprof file")?;
    let file_len = file.metadata()?.len();
    file.seek(SeekFrom::Start(header_bytes_consumed as u64))?;
    let mut work_buf = Vec::with_capacity(SEEK_READ_SIZE * 2);
    let mut batch = Vec::new();
    let mut skipped = 0u64;

    loop {
        let mut pos = 0;
        while pos < work_buf.len() {
            let consumed = extractor.extract(&work_buf[pos..], &mut batch);
            pos += consumed;
            on_batch(&mut batch);
            batch.clear();
            if consumed == 0 {
                break;
            }
        }
        if pos > 0 {
            work_buf.copy_within(pos.., 0);
            work_buf.truncate(work_buf.len() - pos);
        }

        if work_buf.is_empty() {
            let bytes = extractor.skippable_bytes();
            if bytes > 0 {
                let current = file.stream_position()?;
                let end = current
                    .checked_add(bytes as u64)
                    .context("HPROF seek overflow")?;
                anyhow::ensure!(end <= file_len, "truncated HPROF array payload");
                file.seek(SeekFrom::Start(end))?;
                extractor.skip_bytes(bytes)?;
                skipped += bytes as u64;
                continue;
            }
        }

        let old_len = work_buf.len();
        work_buf.resize(old_len + SEEK_READ_SIZE, 0);
        let n = file
            .read(&mut work_buf[old_len..])
            .context("read hprof chunk")?;
        work_buf.truncate(old_len + n);
        if n == 0 {
            anyhow::ensure!(work_buf.is_empty(), "truncated HPROF record at EOF");
            extractor.finish()?;
            eprintln!("  seek reader skipped {skipped} primitive payload bytes");
            return Ok(());
        }
    }
}

// ---------------------------------------------------------------------------
// Header reading
// ---------------------------------------------------------------------------

/// Read and parse the HPROF file header, returning it along with the number
/// of bytes consumed (so the reader can seek past them).
pub fn read_header(path: &Path) -> Result<(FileHeader, usize)> {
    let mut file = File::open(path).context("open hprof file for header")?;
    let mut buf = vec![0u8; 256];
    let n = file.read(&mut buf).context("read hprof header bytes")?;
    buf.truncate(n);

    let (rest, header) =
        parse_file_header(&buf).map_err(|e| anyhow!("failed to parse HPROF file header: {e:?}"))?;

    let consumed = buf.len() - rest.len();
    Ok((header, consumed))
}
