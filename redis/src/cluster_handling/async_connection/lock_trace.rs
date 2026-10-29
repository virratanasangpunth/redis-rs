//! Investigation-only tracing of `conn_lock` (debug-1.7.1-refresh-trace branch; not for
//! upstream). Every acquisition registers its call site; a watchdog thread dumps the whole
//! registry when any waiter has been waiting more than 2 s, which names who holds the lock
//! and who is queued.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, Once};
use std::time::{Duration, Instant};

use log::{debug, warn};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const STUCK_AFTER: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, PartialEq)]
enum State {
    Waiting,
    Held,
}

struct Entry {
    site: &'static str,
    kind: &'static str,
    state: State,
    since: Instant,
    reported: bool,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static REGISTRY: LazyLock<Mutex<HashMap<u64, Entry>>> = LazyLock::new(Default::default);
static WATCHDOG: Once = Once::new();

fn registry() -> std::sync::MutexGuard<'static, HashMap<u64, Entry>> {
    REGISTRY.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Lives for the whole acquisition attempt and the hold; removing the entry on drop also
/// covers a waiter whose future is dropped before it gets the lock.
struct Registration(u64);

impl Registration {
    fn new(site: &'static str, kind: &'static str) -> Self {
        start_watchdog();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        registry().insert(
            id,
            Entry {
                site,
                kind,
                state: State::Waiting,
                since: Instant::now(),
                reported: false,
            },
        );
        Self(id)
    }

    fn held(&self) {
        if let Some(entry) = registry().get_mut(&self.0) {
            entry.state = State::Held;
            entry.since = Instant::now();
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        registry().remove(&self.0);
    }
}

pub(super) struct Traced<G> {
    guard: G,
    /// Set for writes only: the site, the registration id and when it was acquired, so the
    /// release can be logged with its hold time.
    write_release: Option<(&'static str, u64, Instant)>,
    _registration: Registration,
}

impl<G> Drop for Traced<G> {
    fn drop(&mut self) {
        if let Some((site, id, acquired)) = self.write_release {
            debug!(
                "lock-trace: write #{id} {site} released after holding {} ms",
                acquired.elapsed().as_millis()
            );
        }
    }
}

impl<G: Deref> Deref for Traced<G> {
    type Target = G::Target;
    fn deref(&self) -> &G::Target {
        &self.guard
    }
}

impl<G: DerefMut> DerefMut for Traced<G> {
    fn deref_mut(&mut self) -> &mut G::Target {
        &mut self.guard
    }
}

pub(super) async fn read<'a, T>(
    lock: &'a RwLock<T>,
    site: &'static str,
) -> Traced<RwLockReadGuard<'a, T>> {
    let registration = Registration::new(site, "read");
    let guard = lock.read().await;
    registration.held();
    Traced {
        guard,
        write_release: None,
        _registration: registration,
    }
}

pub(super) async fn write<'a, T>(
    lock: &'a RwLock<T>,
    site: &'static str,
) -> Traced<RwLockWriteGuard<'a, T>> {
    let registration = Registration::new(site, "write");
    let id = registration.0;
    let requested = Instant::now();
    debug!("lock-trace: write #{id} {site} requested");
    let guard = lock.write().await;
    registration.held();
    debug!(
        "lock-trace: write #{id} {site} acquired after waiting {} ms",
        requested.elapsed().as_millis()
    );
    Traced {
        guard,
        write_release: Some((site, id, Instant::now())),
        _registration: registration,
    }
}

fn start_watchdog() {
    WATCHDOG.call_once(|| {
        std::thread::spawn(|| {
            loop {
                std::thread::sleep(Duration::from_millis(500));
                let mut entries = registry();
                let stuck = entries.values().any(|entry| {
                    entry.state == State::Waiting
                        && !entry.reported
                        && entry.since.elapsed() > STUCK_AFTER
                });
                if !stuck {
                    continue;
                }
                let mut lines: Vec<(u64, String)> = entries
                    .iter()
                    .map(|(id, entry)| {
                        let state = match entry.state {
                            State::Waiting => "WAITING",
                            State::Held => "held",
                        };
                        (
                            *id,
                            format!(
                                "#{id} {} {} {state} for {} ms",
                                entry.site,
                                entry.kind,
                                entry.since.elapsed().as_millis()
                            ),
                        )
                    })
                    .collect();
                lines.sort_by_key(|(id, _)| *id);
                warn!(
                    "lock-trace: a conn_lock waiter has waited > {} s; registry ({} entries): [{}]",
                    STUCK_AFTER.as_secs(),
                    lines.len(),
                    lines
                        .into_iter()
                        .map(|(_, line)| line)
                        .collect::<Vec<_>>()
                        .join("; ")
                );
                for entry in entries.values_mut() {
                    if entry.state == State::Waiting && entry.since.elapsed() > STUCK_AFTER {
                        entry.reported = true;
                    }
                }
            }
        });
    });
}
