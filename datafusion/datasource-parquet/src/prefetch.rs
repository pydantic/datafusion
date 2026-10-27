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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

static HIT_BYTES: AtomicU64 = AtomicU64::new(0);
static MISS_BYTES: AtomicU64 = AtomicU64::new(0);
static WAIT_US: AtomicU64 = AtomicU64::new(0);
static EVICT_BYTES: AtomicU64 = AtomicU64::new(0);
static DENIED: AtomicU64 = AtomicU64::new(0);

/// Every live prefetch buffer, so a memory pool under pressure can reclaim
/// optional bytes.
static REGISTRY: Mutex<Vec<Weak<Shared>>> = Mutex::new(Vec::new());

/// Drop up to `bytes` of prefetched data that the decoder has not started to
/// read, furthest from the decoder first. Returns the bytes freed. The
/// decoder fetches a dropped range itself when it reaches it.
pub fn reclaim_prefetch(bytes: usize) -> usize {
    let shareds: Vec<Arc<Shared>> = {
        let Ok(mut reg) = REGISTRY.try_lock() else { return 0 };
        reg.retain(|w| w.strong_count() > 0);
        reg.iter().filter_map(Weak::upgrade).collect()
    };
    // Gather candidates across all buffers, then evict the furthest ahead of
    // each buffer's read position first, round robin by distance.
    let mut cands: Vec<(u64, usize, u64)> = Vec::new(); // (distance, shared idx, start)
    for (i, s) in shareds.iter().enumerate() {
        let Ok(entries) = s.entries.try_lock() else { continue };
        let pos = entries.keys().next().copied().unwrap_or(0);
        for (start, (_, e)) in entries.iter() {
            if let Entry::Ready { consumed: 0, .. } = e {
                cands.push((start - pos, i, *start));
            }
        }
    }
    cands.sort_by(|a, b| b.0.cmp(&a.0));
    let mut freed = 0;
    for (_, i, start) in cands {
        if freed >= bytes {
            break;
        }
        let s = &shareds[i];
        let Ok(mut entries) = s.entries.try_lock() else { continue };
        if let Some((end, Entry::Ready { consumed: 0, .. })) = entries.get(&start) {
            let len = (*end - start) as usize;
            entries.remove(&start);
            drop(entries);
            s.reservation.shrink(len);
            freed += len;
        }
    }
    EVICT_BYTES.fetch_add(freed as u64, Ordering::Relaxed);
    freed
}


use bytes::Bytes;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use futures::FutureExt;
use futures::future::BoxFuture;
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
    reservation: MemoryReservation,
    notify: Notify,
}

pub(crate) struct PrefetchReader {
    inner: Box<dyn AsyncFileReader>,
    shared: Arc<Shared>,
    partition: usize,
    seq: u64,
}

/// Per partition: file sequence number -> whether that file has issued all
/// its fetches. A file fetches only after every earlier open file of the
/// same partition has issued all its fetches, so the file being decoded
/// gets I/O and memory before look-ahead files.
static PRIORITY: Mutex<BTreeMap<(usize, u64), bool>> = Mutex::new(BTreeMap::new());
static SEQ: AtomicU64 = AtomicU64::new(0);

fn may_fetch(partition: usize, seq: u64) -> bool {
    PRIORITY
        .lock()
        .unwrap()
        .range((partition, 0)..(partition, seq))
        .all(|(_, done)| *done)
}

fn set_done(partition: usize, seq: u64) {
    if let Some(d) = PRIORITY.lock().unwrap().get_mut(&(partition, seq)) {
        *d = true;
    }
}

impl PrefetchReader {
    /// Spawn one prefetch worker per reader in `prefetch_readers` for
    /// `plan`, and wrap `inner` so it reads from the prefetched bytes.
    pub(crate) fn spawn(
        inner: Box<dyn AsyncFileReader>,
        prefetch_readers: Vec<Box<dyn AsyncFileReader>>,
        plan: ScanPlan,
        pool: Arc<dyn MemoryPool>,
        partition: usize,
    ) -> Self {
        let reservation =
            MemoryConsumer::new(format!("ParquetPrefetch[{partition}]")).register(&pool);
        let shared = Arc::new(Shared {
            entries: Mutex::new(BTreeMap::new()),
            reservation,
            notify: Notify::new(),
        });
        let fraction = env_f64("DF_PREFETCH_FRACTION").unwrap_or(0.6);
        let fallible = std::env::var("DF_PREFETCH_TRY").is_ok();
        let chunk = env_usize("DF_PREFETCH_CHUNK").unwrap_or(8 << 20) as u64;
        REGISTRY.lock().unwrap().push(Arc::downgrade(&shared));
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        PRIORITY.lock().unwrap().insert((partition, seq), false);
        let ranges = Arc::new(coalesce(plan, chunk));
        let next = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(AtomicUsize::new(prefetch_readers.len()));
        for reader in prefetch_readers {
            tokio::spawn(prefetch_worker(
                Arc::downgrade(&shared),
                reader,
                Arc::clone(&ranges),
                Arc::clone(&next),
                Arc::clone(&live),
                Arc::clone(&pool),
                fraction,
                fallible,
                partition,
                seq,
            ));
        }
        Self {
            inner,
            shared,
            partition,
            seq,
        }
    }
}

impl Drop for PrefetchReader {
    fn drop(&mut self) {
        PRIORITY.lock().unwrap().remove(&(self.partition, self.seq));
        // The workers hold only a `Weak`. Waking them lets them observe the drop.
        self.shared.notify.notify_waiters();
        if std::env::var("DF_PREFETCH_STATS").is_ok() {
            eprintln!(
                "PREFETCH_STATS hit_mib={} miss_mib={} wait_ms={} evict_mib={} denied={}",
                HIT_BYTES.load(Ordering::Relaxed) >> 20,
                MISS_BYTES.load(Ordering::Relaxed) >> 20,
                WAIT_US.load(Ordering::Relaxed) / 1000,
                EVICT_BYTES.load(Ordering::Relaxed) >> 20,
                DENIED.load(Ordering::Relaxed)
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

/// One worker keeps one request in flight. Each worker takes the next range
/// in decode order as soon as its previous request completes, so a slow
/// range does not hold back the others.
#[allow(clippy::too_many_arguments)]
async fn prefetch_worker(
    shared: Weak<Shared>,
    mut reader: Box<dyn AsyncFileReader>,
    ranges: Arc<Vec<Range<u64>>>,
    next: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
    pool: Arc<dyn MemoryPool>,
    fraction: f64,
    fallible: bool,
    partition: usize,
    seq: u64,
) {
    loop {
        let Some(s) = shared.upgrade() else { return };
        let i = next.load(Ordering::Relaxed);
        if i >= ranges.len() {
            if live.fetch_sub(1, Ordering::Relaxed) == 1 {
                set_done(partition, seq);
            }
            return;
        }
        let r = ranges[i].clone();
        let len = (r.end - r.start) as usize;
        if !may_fetch(partition, seq) || !pool_has_room(pool.as_ref(), fraction, len) {
            // Wait for consumption, or poll again soon, because other
            // partitions and earlier files also free memory and I/O.
            let _ =
                tokio::time::timeout(Duration::from_millis(2), s.notify.notified()).await;
            continue;
        }
        if fallible {
            // Optional bytes: take only what the pool grants now.
            if s.reservation.try_grow(len).is_err() {
                DENIED.fetch_add(1, Ordering::Relaxed);
                let _ = tokio::time::timeout(Duration::from_millis(5), s.notify.notified())
                    .await;
                continue;
            }
        } else {
            s.reservation.grow(len);
        }
        if next
            .compare_exchange(i, i + 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            s.reservation.shrink(len);
            continue;
        }
        if i + 1 == ranges.len() {
            set_done(partition, seq);
        }
        s.entries
            .lock()
            .unwrap()
            .insert(r.start, (r.end, Entry::InFlight));
        drop(s);
        let result = reader.get_bytes(r.clone()).await;
        let Some(s) = shared.upgrade() else { return };
        {
            let mut entries = s.entries.lock().unwrap();
            match result {
                Ok(b) => {
                    entries.insert(
                        r.start,
                        (
                            r.end,
                            Entry::Ready {
                                bytes: b,
                                consumed: 0,
                            },
                        ),
                    );
                }
                Err(_) => {
                    // Let the decoder fetch this range itself.
                    entries.remove(&r.start);
                    s.reservation.shrink(len);
                }
            }
        }
        s.notify.notify_waiters();
    }
}

enum Lookup {
    Miss,
    Wait,
    Hit(Bytes),
}

impl Shared {
    /// Serve `range` from one or more contiguous entries.
    fn lookup(&self, range: &Range<u64>) -> Lookup {
        let mut entries = self.entries.lock().unwrap();
        let Some((&first, _)) = entries.range(..=range.start).next_back() else {
            return Lookup::Miss;
        };
        let mut parts = Vec::new();
        let mut pos = range.start;
        for (&start, (end, entry)) in entries.range(first..) {
            if start > pos {
                return Lookup::Miss;
            }
            if *end <= pos {
                continue;
            }
            match entry {
                Entry::InFlight => return Lookup::Wait,
                Entry::Ready { .. } => parts.push(start),
            }
            pos = *end;
            if pos >= range.end {
                break;
            }
        }
        if pos < range.end {
            return Lookup::Miss;
        }
        let mut out = bytes::BytesMut::new();
        let mut single = None;
        let mut freed = 0;
        for start in &parts {
            let (end, entry) = entries.get_mut(start).unwrap();
            let Entry::Ready { bytes, consumed } = entry else {
                unreachable!()
            };
            let lo = range.start.max(*start);
            let hi = range.end.min(*end);
            let piece = bytes.slice((lo - start) as usize..(hi - start) as usize);
            if parts.len() == 1 {
                single = Some(piece);
            } else {
                out.extend_from_slice(&piece);
            }
            *consumed += hi - lo;
            if *consumed >= *end - start {
                freed += (*end - start) as usize;
                entries.remove(start);
            }
        }
        if freed > 0 {
            self.reservation.shrink(freed);
            self.notify.notify_waiters();
        }
        Lookup::Hit(single.unwrap_or_else(|| out.freeze()))
    }
}

impl PrefetchReader {
    async fn read(&mut self, range: Range<u64>) -> ParquetResult<Bytes> {
        loop {
            let notified = self.shared.notify.notified();
            match self.shared.lookup(&range) {
                Lookup::Hit(b) => {
                    HIT_BYTES.fetch_add(range.end - range.start, Ordering::Relaxed);
                    return Ok(b);
                }
                Lookup::Miss => {
                    MISS_BYTES.fetch_add(range.end - range.start, Ordering::Relaxed);
                    return self.inner.get_bytes(range).await;
                }
                Lookup::Wait => {
                    let t = std::time::Instant::now();
                    notified.await;
                    WAIT_US.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                }
            }
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
            for r in ranges {
                out.push(self.read(r).await?);
            }
            Ok(out)
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
