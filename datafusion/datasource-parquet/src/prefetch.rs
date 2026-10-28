// PROTOTYPE: a fetch window over one file's scan plan.
//
// The decoder's `scan_plan()` lists the byte ranges of the file in decode
// order. This module coalesces them into chunks and keeps a window of chunks
// in memory, starting at the read cursor. The cursor is the chunk that holds
// the decoder's current read.
//
// Each chunk has one owner at a time, so no chunk is fetched twice:
//
// - The decoder fetches a chunk itself when it reads from a chunk that no
//   worker has claimed. These are required bytes, so the reservation grows
//   without a check.
// - Workers fetch chunks ahead of the cursor, up to `budget` bytes from the
//   cursor. These are optional bytes, so the reservation uses `try_grow` and
//   a worker waits when the pool refuses.
//
// When the cursor moves past a chunk, the window frees the chunk. A chunk
// that is in flight at that time is freed when its fetch completes. Dropping
// the reader aborts the workers and frees the reservation.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::ScanPlan;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ParquetMetaData;
use tokio::sync::Notify;
use tokio::task::JoinSet;

static HIT_BYTES: AtomicU64 = AtomicU64::new(0);
static MISS_BYTES: AtomicU64 = AtomicU64::new(0);
static REQUIRED_BYTES: AtomicU64 = AtomicU64::new(0);
static WAIT_US: AtomicU64 = AtomicU64::new(0);
static DENIED: AtomicU64 = AtomicU64::new(0);

pub(crate) fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// The settings of a prefetch window. The opener reads them once per file.
#[derive(Clone, Copy)]
pub(crate) struct PrefetchConfig {
    /// The largest chunk, in bytes.
    pub chunk: u64,
    /// The most bytes that workers hold or fetch, counted from the cursor.
    pub budget: u64,
    /// The number of workers. Each worker has one request in flight.
    pub concurrency: usize,
}

impl PrefetchConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            chunk: env_usize("DF_PREFETCH_CHUNK").unwrap_or(8 << 20) as u64,
            budget: env_usize("DF_PREFETCH_BUDGET").unwrap_or(32 << 20) as u64,
            concurrency: env_usize("DF_PREFETCH_CONCURRENCY").unwrap_or(4),
        }
    }
}

enum State {
    Pending,
    InFlight,
    Ready(Bytes),
    Freed,
}

struct Chunk {
    range: Range<u64>,
    state: State,
}

struct Window {
    chunks: Vec<Chunk>,
    /// The index of the chunk that holds the decoder's current read. Each
    /// chunk before it is `Freed`, or `InFlight` and freed on completion.
    cursor: usize,
}

struct Shared {
    window: Mutex<Window>,
    reservation: MemoryReservation,
    notify: Notify,
    budget: u64,
}

pub(crate) struct PrefetchReader {
    inner: Box<dyn AsyncFileReader>,
    shared: Arc<Shared>,
    /// Dropping the set aborts the workers.
    _workers: JoinSet<()>,
}

impl PrefetchReader {
    /// Wrap `inner` in a window over `plan`, with one worker for each reader
    /// in `prefetch_readers`. Return `inner` unchanged if the plan does not
    /// fit a window.
    pub(crate) fn spawn(
        inner: Box<dyn AsyncFileReader>,
        prefetch_readers: Vec<Box<dyn AsyncFileReader>>,
        plan: ScanPlan,
        pool: Arc<dyn MemoryPool>,
        partition: usize,
        config: PrefetchConfig,
    ) -> Result<Self, Box<dyn AsyncFileReader>> {
        let Some(ranges) = coalesce(plan, config.chunk) else {
            // The plan moves backward. Read without prefetch.
            return Err(inner);
        };
        let chunks = ranges
            .into_iter()
            .map(|range| Chunk {
                range,
                state: State::Pending,
            })
            .collect();
        let shared = Arc::new(Shared {
            window: Mutex::new(Window { chunks, cursor: 0 }),
            reservation: MemoryConsumer::new(format!("ParquetPrefetch[{partition}]"))
                .register(&pool),
            notify: Notify::new(),
            budget: config.budget,
        });
        let mut workers = JoinSet::new();
        for reader in prefetch_readers {
            workers.spawn(worker(Arc::clone(&shared), reader));
        }
        Ok(Self {
            inner,
            shared,
            _workers: workers,
        })
    }
}

impl Drop for PrefetchReader {
    fn drop(&mut self) {
        if std::env::var("DF_PREFETCH_STATS").is_ok() {
            eprintln!(
                "PREFETCH_STATS hit_mib={} miss_mib={} required_mib={} wait_ms={} denied={}",
                HIT_BYTES.load(Ordering::Relaxed) >> 20,
                MISS_BYTES.load(Ordering::Relaxed) >> 20,
                REQUIRED_BYTES.load(Ordering::Relaxed) >> 20,
                WAIT_US.load(Ordering::Relaxed) / 1000,
                DENIED.load(Ordering::Relaxed)
            );
        }
    }
}

/// Merge adjacent plan ranges into chunks of at most `chunk` bytes, and split
/// larger ranges. The window needs chunks in increasing byte order, so return
/// `None` if the plan moves backward.
fn coalesce(plan: ScanPlan, chunk: u64) -> Option<Vec<Range<u64>>> {
    let mut out: Vec<Range<u64>> = Vec::new();
    for planned in plan {
        let mut r = planned.range;
        if let Some(last) = out.last() {
            if r.start < last.start {
                return None;
            }
            // Keep only the part after the previous chunk.
            r.start = r.start.max(last.end);
        }
        while r.start < r.end {
            match out.last_mut() {
                Some(last) if last.end == r.start && last.end - last.start < chunk => {
                    let end = r.end.min(last.start + chunk);
                    last.end = end;
                    r.start = end;
                }
                _ => {
                    let end = r.end.min(r.start + chunk);
                    out.push(r.start..end);
                    r.start = end;
                }
            }
        }
    }
    Some(out)
}

/// A claim on an `InFlight` chunk. Dropping an unfinished claim returns the
/// chunk to `Pending` and releases its memory, so no reader waits forever.
struct Claim<'a> {
    shared: &'a Shared,
    i: usize,
    done: bool,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut w = self.shared.window.lock().unwrap();
        let chunk = &mut w.chunks[self.i];
        if matches!(chunk.state, State::InFlight) {
            chunk.state = State::Pending;
        }
        self.shared.reservation.shrink(len(&chunk.range));
        drop(w);
        self.shared.notify.notify_waiters();
    }
}

fn len(r: &Range<u64>) -> usize {
    (r.end - r.start) as usize
}

impl Shared {
    /// Release the memory of `chunk`. If the chunk is in flight, `complete`
    /// releases the memory when the fetch ends.
    fn free(&self, chunk: &mut Chunk) {
        if let State::Ready(_) = std::mem::replace(&mut chunk.state, State::Freed) {
            self.reservation.shrink(len(&chunk.range));
        }
    }

    /// Fetch the claimed chunk `i` and store the result. If the future is
    /// dropped first, the guard returns the chunk to `Pending`.
    async fn fetch(
        self: &Arc<Self>,
        reader: &mut Box<dyn AsyncFileReader>,
        i: usize,
        range: Range<u64>,
    ) {
        let mut guard = Claim {
            shared: self,
            i,
            done: false,
        };
        let result = reader.get_bytes(range).await;
        guard.done = true;
        self.complete(i, result);
    }

    /// Store the result of the fetch of chunk `i`.
    fn complete(&self, i: usize, result: ParquetResult<Bytes>) {
        let mut w = self.window.lock().unwrap();
        let chunk = &mut w.chunks[i];
        match (&chunk.state, result) {
            (State::InFlight, Ok(bytes)) => chunk.state = State::Ready(bytes),
            (State::InFlight, Err(_)) => {
                // The decoder reads this range directly.
                chunk.state = State::Freed;
                self.reservation.shrink(len(&chunk.range));
            }
            // The cursor moved past the chunk while it was in flight.
            _ => self.reservation.shrink(len(&chunk.range)),
        }
        drop(w);
        self.notify.notify_waiters();
    }

    /// Claim the first pending chunk ahead of the cursor for a worker. The
    /// bytes after the cursor chunk up to the end of that chunk must fit in the budget,
    /// and the pool must grant them. Return `Err` when no pending chunk is
    /// left.
    fn claim_optional(&self) -> Result<Option<(usize, Range<u64>)>, ()> {
        let mut w = self.window.lock().unwrap();
        let cursor = w.cursor;
        let mut ahead = 0;
        for i in cursor..w.chunks.len() {
            if i > cursor {
                ahead += len(&w.chunks[i].range) as u64;
            }
            if !matches!(w.chunks[i].state, State::Pending) {
                continue;
            }
            if ahead > self.budget {
                return Ok(None);
            }
            let r = w.chunks[i].range.clone();
            if self.reservation.try_grow(len(&r)).is_err() {
                DENIED.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            w.chunks[i].state = State::InFlight;
            return Ok(Some((i, r)));
        }
        Err(())
    }

    /// Move the cursor to the chunk that holds `range.start`, then serve
    /// `range` or say what the decoder must do first.
    fn step(&self, range: &Range<u64>) -> Step {
        let mut guard = self.window.lock().unwrap();
        let w = &mut *guard;
        let Some(first) = (w.cursor..w.chunks.len())
            .find(|&i| w.chunks[i].range.contains(&range.start))
        else {
            return Step::Direct;
        };
        if first > w.cursor {
            for i in w.cursor..first {
                self.free(&mut w.chunks[i]);
            }
            w.cursor = first;
            // The budget now reaches further, so wake the workers.
            self.notify.notify_waiters();
        }
        let mut last = first;
        while w.chunks[last].range.end < range.end {
            last += 1;
            if last == w.chunks.len()
                || w.chunks[last].range.start != w.chunks[last - 1].range.end
            {
                // The range leaves the plan. Read it directly.
                return Step::Direct;
            }
        }
        let mut claims = Vec::new();
        let mut wait = false;
        for i in first..=last {
            match w.chunks[i].state {
                State::Pending => {
                    // Required bytes: the decoder cannot continue without them.
                    let r = w.chunks[i].range.clone();
                    self.reservation.grow(len(&r));
                    REQUIRED_BYTES.fetch_add(r.end - r.start, Ordering::Relaxed);
                    w.chunks[i].state = State::InFlight;
                    claims.push((i, r));
                }
                State::InFlight => wait = true,
                State::Ready(_) => {}
                State::Freed => return Step::Direct,
            }
        }
        if !claims.is_empty() {
            return Step::Fetch(claims);
        }
        if wait {
            return Step::Wait;
        }
        let mut pieces = (first..=last).map(|i| {
            let c = &w.chunks[i];
            let State::Ready(bytes) = &c.state else {
                unreachable!()
            };
            let lo = range.start.max(c.range.start) - c.range.start;
            let hi = range.end.min(c.range.end) - c.range.start;
            bytes.slice(lo as usize..hi as usize)
        });
        if first == last {
            return Step::Hit(pieces.next().unwrap());
        }
        let mut out = bytes::BytesMut::with_capacity(len(range));
        pieces.for_each(|p| out.extend_from_slice(&p));
        Step::Hit(out.freeze())
    }
}

/// Fetch optional chunks until none is pending.
async fn worker(shared: Arc<Shared>, mut reader: Box<dyn AsyncFileReader>) {
    loop {
        let notified = shared.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        match shared.claim_optional() {
            Err(()) => return,
            Ok(Some((i, range))) => shared.fetch(&mut reader, i, range).await,
            // The cursor moves with a notification, but the pool frees memory
            // without one. So poll the pool as well.
            Ok(None) => {
                let _ = tokio::time::timeout(Duration::from_millis(5), notified).await;
            }
        }
    }
}

enum Step {
    Hit(Bytes),
    Wait,
    Fetch(Vec<(usize, Range<u64>)>),
    Direct,
}

impl PrefetchReader {
    async fn read(&mut self, range: Range<u64>) -> ParquetResult<Bytes> {
        let shared = Arc::clone(&self.shared);
        loop {
            let notified = shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match shared.step(&range) {
                Step::Hit(b) => {
                    HIT_BYTES.fetch_add(range.end - range.start, Ordering::Relaxed);
                    return Ok(b);
                }
                Step::Direct => {
                    MISS_BYTES.fetch_add(range.end - range.start, Ordering::Relaxed);
                    return self.inner.get_bytes(range).await;
                }
                Step::Wait => {
                    let t = std::time::Instant::now();
                    notified.await;
                    WAIT_US.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                }
                Step::Fetch(claims) => {
                    // The decoder has one reader, so it fetches the claims in
                    // turn. A read spans more than one chunk only at a chunk
                    // boundary, so a claim list is short.
                    for (i, r) in claims {
                        shared.fetch(&mut self.inner, i, r).await;
                    }
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
