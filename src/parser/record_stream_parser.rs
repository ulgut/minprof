// SPDX-License-Identifier: Apache-2.0
//
// Adapted from hprof-slurp <https://github.com/agourlay/hprof-slurp>
// Copyright (c) Arnaud Gourlay and hprof-slurp contributors.
// Licensed under the Apache License, Version 2.0.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::thread;

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::bounded;

use crate::parser::file_header_parser::{FileHeader, parse_file_header};

pub const READ_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Max chunks buffered between adjacent pipeline stages.
const POOL_DEPTH: usize = 3;

/// Like [`process`] but instead of delivering records to a visitor, runs
/// `extractor` in the parser thread to populate a batch of `T` values, then
/// delivers each batch to `on_batch` on the main thread.
///
/// `extractor(buf, batch) -> usize` should append any values derived from `buf`
/// to `batch` and return the number of bytes consumed from `buf`. Returning 0
/// means "need more data" (Incomplete — the buffer will be grown before the
/// next call).
pub fn process_with_extractor<T, E>(
    path: &Path,
    extractor: E,
    on_batch: &mut dyn FnMut(&mut Vec<T>),
) -> Result<()>
where
    T: Send + 'static,
    E: FnMut(&[u8], &mut Vec<T>) -> usize + Send + 'static,
{
    let (_, header_bytes_consumed) = read_header(path)?;
    let path = path.to_path_buf();

    let (send_chunk, recv_chunk) = bounded::<Vec<u8>>(POOL_DEPTH);
    let (send_pooled_buf, recv_pooled_buf) = bounded::<Vec<u8>>(POOL_DEPTH);
    let (send_batch, recv_batch) = bounded::<Vec<T>>(POOL_DEPTH);
    let (send_pooled_batch, recv_pooled_batch) = bounded::<Vec<T>>(POOL_DEPTH);

    for _ in 0..POOL_DEPTH {
        send_pooled_buf
            .send(Vec::with_capacity(READ_BUFFER_SIZE))
            .unwrap();
        send_pooled_batch.send(Vec::new()).unwrap();
    }

    // -- Reader thread --------------------------------------------------------
    let reader = thread::Builder::new().name("hprof-reader".into()).spawn(
        move || -> Result<()> {
            let mut file = File::open(path).context("open hprof file")?;
            file.seek(SeekFrom::Start(header_bytes_consumed as u64))
                .context("seek past header")?;

            let mut chunk_count = 0u64;
            let mut total_bytes = 0u64;
            let mut partial_count = 0u64;
            let t0 = std::time::Instant::now();

            loop {
                let mut buf = recv_pooled_buf
                    .recv()
                    .expect("pool channel closed prematurely");
                buf.resize(READ_BUFFER_SIZE, 0);
                let n = file.read(&mut buf).context("read hprof chunk")?;
                if n == 0 {
                    break;
                }

                chunk_count += 1;
                total_bytes += n as u64;
                if n < READ_BUFFER_SIZE {
                    partial_count += 1;
                }
                if chunk_count % 64 == 0 {
                    let elapsed = t0.elapsed().as_secs_f64();
                    eprintln!(
                        "  reader: {chunk_count} reads, {:.1} GiB, {:.0} partial, \
                         {:.0} MiB/s, avg read size {:.1} MiB",
                        total_bytes as f64 / (1 << 30) as f64,
                        partial_count,
                        total_bytes as f64 / elapsed / (1 << 20) as f64,
                        total_bytes as f64 / chunk_count as f64 / (1 << 20) as f64,
                    );
                }

                buf.truncate(n);
                send_chunk.send(buf).expect("extractor channel closed");
            }

            let elapsed = t0.elapsed().as_secs_f64();
            eprintln!(
                "  reader done: {chunk_count} reads, {:.1} GiB, {partial_count} partial ({:.1}%), \
                 avg {:.1} MiB/read, {:.0} MiB/s",
                total_bytes as f64 / (1 << 30) as f64,
                partial_count as f64 / chunk_count.max(1) as f64 * 100.0,
                total_bytes as f64 / chunk_count.max(1) as f64 / (1 << 20) as f64,
                total_bytes as f64 / elapsed / (1 << 20) as f64,
            );
            Ok(())
        },
    )?;

    // -- Extractor thread -----------------------------------------------------
    let extractor_thread = thread::Builder::new()
        .name("hprof-extractor".into())
        .spawn(move || {
            let mut extract_fn = extractor;
            let mut work_buf: Vec<u8> = Vec::new();
            let mut work_pos: usize = 0;

            while let Ok(mut chunk) = recv_chunk.recv() {
                work_buf.extend_from_slice(&chunk);
                chunk.clear();
                let _ = send_pooled_buf.send(chunk);

                let mut batch = recv_pooled_batch.recv().expect("batch pool closed");

                let consumed = extract_fn(&work_buf[work_pos..], &mut batch);
                work_pos += consumed;

                // Compact: shift tail to front once ≥ half the buffer is consumed.
                if work_pos > 0 && work_pos >= work_buf.len() / 2 {
                    let tail = work_buf.len() - work_pos;
                    work_buf.copy_within(work_pos.., 0);
                    work_buf.truncate(tail);
                    work_pos = 0;
                }

                send_batch.send(batch).expect("main batch channel closed");
            }
        })?;

    // -- Main thread: consume batches -----------------------------------------
    for mut batch in recv_batch {
        on_batch(&mut batch);
        batch.clear();
        let _ = send_pooled_batch.send(batch);
    }

    reader
        .join()
        .expect("reader thread panicked")
        .context("reader thread error")?;
    extractor_thread.join().expect("extractor thread panicked");

    Ok(())
}

// ---------------------------------------------------------------------------
// Header reading
// ---------------------------------------------------------------------------

/// Read and parse the HPROF file header, returning it along with the number
/// of bytes consumed (so the reader thread can seek past them).
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
