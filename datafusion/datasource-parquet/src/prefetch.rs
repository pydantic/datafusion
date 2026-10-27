// PROTOTYPE: background prefetch of a file's scan plan into the memory pool.
//
// A spawned task fetches the decoder's `scan_plan()` ranges in decode order,
// independent of whether the stream is being polled. The decoder's reader
// serves requests from the prefetched bytes when they cover the request.
// The task reserves every byte it holds from the memory pool, and stops
// fetching while the pool is above `DF_PREFETCH_FRACTION` of its limit, so
// other operators keep headroom.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static HIT_BYTES: AtomicU64 = AtomicU64::new(0);
static MISS_BYTES: AtomicU64 = AtomicU64::new(0);
static WAIT_US: AtomicU64 = AtomicU64::new(0);

use bytes::Bytes;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::ScanPlan;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ParquetMetaData;
use tokio::sync::Notify;

pub(crate) fn env_f64(name: &str) -> Option<f64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

pub(crate) fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

enum Entry {
    InFlight,
    Ready { bytes: Bytes, consumed: u64 },
}

struct Shared {
    /// start -> (end, entry)
    entries: Mutex<BTreeMap<u64, (u64, Entry)>>,
    reservation: Mutex<MemoryReservation>,
    notify: Notify,
}

impl Shared {
    /// Find an entry that fully covers `range`.
    fn covering(&self, range: &Range<u64>) -> Option<u64> {
        let entries = self.entries.lock().unwrap();
        let (start, (end, _)) = entries.range(..=range.start).next_back()?;
        (*end >= range.end).then_some(*start)
    }
}

pub(crate) struct PrefetchReader {
    inner: Box<dyn AsyncFileReader>,
    shared: Arc<Shared>,
}

impl PrefetchReader {
    /// Spawn a prefetch task for `plan`, using `prefetch_reader` for its I/O,
    /// and wrap `inner` so it reads from the prefetched bytes.
    pub(crate) fn spawn(
        inner: Box<dyn AsyncFileReader>,
        prefetch_reader: Box<dyn AsyncFileReader>,
        plan: ScanPlan,
        pool: Arc<dyn MemoryPool>,
        partition: usize,
    ) -> Self {
        let reservation = MemoryConsumer::new(format!("ParquetPrefetch[{partition}]"))
            .register(&pool);
        let shared = Arc::new(Shared {
            entries: Mutex::new(BTreeMap::new()),
            reservation: Mutex::new(reservation),
            notify: Notify::new(),
        });
        let fraction = env_f64("DF_PREFETCH_FRACTION").unwrap_or(0.6);
        let concurrency = env_usize("DF_PREFETCH_CONCURRENCY").unwrap_or(4);
        let chunk = env_usize("DF_PREFETCH_CHUNK").unwrap_or(8 << 20) as u64;
        tokio::spawn(prefetch_task(
            Arc::downgrade(&shared),
            prefetch_reader,
            coalesce(plan, chunk),
            pool,
            fraction,
            concurrency,
        ));
        Self { inner, shared }
    }
}

impl Drop for PrefetchReader {
    fn drop(&mut self) {
        // The task holds only a `Weak`. Waking it lets it observe the drop.
        self.shared.notify.notify_waiters();
        if std::env::var("DF_PREFETCH_STATS").is_ok() {
            eprintln!(
                "PREFETCH_STATS hit_mib={} miss_mib={} wait_ms={}",
                HIT_BYTES.load(Ordering::Relaxed) >> 20,
                MISS_BYTES.load(Ordering::Relaxed) >> 20,
                WAIT_US.load(Ordering::Relaxed) / 1000
            );
        }
    }
}

/// Merge adjacent plan ranges into fetches of at most `chunk` bytes.
fn coalesce(plan: ScanPlan, chunk: u64) -> Vec<Range<u64>> {
    let mut out: Vec<Range<u64>> = Vec::new();
    for planned in plan {
        let r = planned.range;
        if let Some(last) = out.last_mut() {
            if r.start == last.end && last.end - last.start + (r.end - r.start) <= chunk {
                last.end = r.end;
                continue;
            }
            if r.start >= last.start && r.end <= last.end {
                continue;
            }
        }
        out.push(r);
    }
    out
}

fn pool_has_room(pool: &dyn MemoryPool, fraction: f64, add: usize) -> bool {
    match pool.memory_limit() {
        MemoryLimit::Finite(limit) => {
            (pool.reserved() + add) as f64 <= limit as f64 * fraction
        }
        _ => true,
    }
}

async fn prefetch_task(
    shared: Weak<Shared>,
    mut reader: Box<dyn AsyncFileReader>,
    ranges: Vec<Range<u64>>,
    pool: Arc<dyn MemoryPool>,
    fraction: f64,
    concurrency: usize,
) {
    // One reader, so fetches run one at a time on it. Run `concurrency`
    // requests by batching them into one `get_byte_ranges` call.
    let mut next = 0;
    while next < ranges.len() {
        let Some(s) = shared.upgrade() else { return };
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        while next < ranges.len() && batch.len() < concurrency {
            let r = ranges[next].clone();
            let len = (r.end - r.start) as usize;
            if !pool_has_room(pool.as_ref(), fraction, bytes + len) {
                break;
            }
            bytes += len;
            batch.push(r);
            next += 1;
        }
        if batch.is_empty() {
            // No room. Wait for consumption, or poll the pool again soon,
            // because other partitions also free memory.
            let _ = tokio::time::timeout(Duration::from_millis(5), s.notify.notified())
                .await;
            continue;
        }
        s.reservation.lock().unwrap().grow(bytes);
        {
            let mut entries = s.entries.lock().unwrap();
            for r in &batch {
                entries.insert(r.start, (r.end, Entry::InFlight));
            }
        }
        drop(s);
        let result = reader.get_byte_ranges(batch.clone()).await;
        let Some(s) = shared.upgrade() else { return };
        let mut entries = s.entries.lock().unwrap();
        match result {
            Ok(data) => {
                for (r, b) in batch.iter().zip(data) {
                    entries.insert(r.start, (r.end, Entry::Ready { bytes: b, consumed: 0 }));
                }
            }
            Err(_) => {
                // Let the decoder fetch these itself.
                for r in &batch {
                    entries.remove(&r.start);
                }
                s.reservation.lock().unwrap().shrink(bytes);
            }
        }
        drop(entries);
        s.notify.notify_waiters();
    }
}

impl PrefetchReader {
    async fn read(&mut self, range: Range<u64>) -> ParquetResult<Bytes> {
        loop {
            let Some(start) = self.shared.covering(&range) else {
                MISS_BYTES.fetch_add(range.end - range.start, Ordering::Relaxed);
                return self.inner.get_bytes(range).await;
            };
            let notified = self.shared.notify.notified();
            {
                let mut entries = self.shared.entries.lock().unwrap();
                match entries.get_mut(&start) {
                    Some((end, Entry::Ready { bytes, consumed })) => {
                        let off = (range.start - start) as usize;
                        let out = bytes.slice(off..off + (range.end - range.start) as usize);
                        *consumed += range.end - range.start;
                        HIT_BYTES.fetch_add(range.end - range.start, Ordering::Relaxed);
                        if *consumed >= *end - start {
                            let len = (*end - start) as usize;
                            entries.remove(&start);
                            self.shared.reservation.lock().unwrap().shrink(len);
                            self.shared.notify.notify_waiters();
                        }
                        return Ok(out);
                    }
                    Some((_, Entry::InFlight)) => {}
                    None => continue,
                }
            }
            let t = std::time::Instant::now();
            notified.await;
            WAIT_US.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
    }
}

impl AsyncFileReader for PrefetchReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.read(range).boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            let mut out = Vec::with_capacity(ranges.len());
            let mut misses = Vec::new();
            for (i, r) in ranges.iter().enumerate() {
                if self.shared.covering(r).is_some() {
                    out.push(Some(self.read(r.clone()).await?));
                } else {
                    out.push(None);
                    misses.push(i);
                }
            }
            if !misses.is_empty() {
                let fetched = self
                    .inner
                    .get_byte_ranges(misses.iter().map(|i| ranges[*i].clone()).collect())
                    .await?;
                MISS_BYTES.fetch_add(
                    misses.iter().map(|i| ranges[*i].end - ranges[*i].start).sum(),
                    Ordering::Relaxed,
                );
                for (i, b) in misses.into_iter().zip(fetched) {
                    out[i] = Some(b);
                }
            }
            Ok(out.into_iter().map(Option::unwrap).collect())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}

