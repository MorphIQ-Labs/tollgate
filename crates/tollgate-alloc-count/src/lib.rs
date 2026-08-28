//! Scoped, current-thread allocation attribution for tests and benchmarks.
//!
//! The crate is deliberately dev-only. Test binaries install
//! [`CountingAllocator`] as their global allocator, warm dependency-owned
//! thread-local state, and then use [`AllocScope`] to distinguish Tollgate's
//! steady-state work from caller and executor allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;

use serde::Serialize;

/// The report path understood by [`record_if_requested!`].
pub const REPORT_ENV: &str = "TOLLGATE_ALLOC_REPORT";

/// Heap operations observed on the measuring thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Allocations {
    pub alloc_calls: u64,
    pub alloc_zeroed_calls: u64,
    pub realloc_calls: u64,
    pub dealloc_calls: u64,
    pub allocated_bytes: u64,
    pub reallocated_bytes: u64,
    pub deallocated_bytes: u64,
}

impl Allocations {
    /// Whether the scope requested no new or resized heap storage.
    #[must_use]
    pub const fn is_allocation_free(self) -> bool {
        self.alloc_calls == 0 && self.alloc_zeroed_calls == 0 && self.realloc_calls == 0
    }

    fn difference(self, before: Self) -> Self {
        Self {
            alloc_calls: self.alloc_calls.saturating_sub(before.alloc_calls),
            alloc_zeroed_calls: self
                .alloc_zeroed_calls
                .saturating_sub(before.alloc_zeroed_calls),
            realloc_calls: self.realloc_calls.saturating_sub(before.realloc_calls),
            dealloc_calls: self.dealloc_calls.saturating_sub(before.dealloc_calls),
            allocated_bytes: self.allocated_bytes.saturating_sub(before.allocated_bytes),
            reallocated_bytes: self
                .reallocated_bytes
                .saturating_sub(before.reallocated_bytes),
            deallocated_bytes: self
                .deallocated_bytes
                .saturating_sub(before.deallocated_bytes),
        }
    }
}

thread_local! {
    // Const initialisers avoid lazy heap-backed TLS setup inside the allocator.
    static ACTIVE_DEPTH: Cell<u32> = const { Cell::new(0) };
    static COUNTS: Cell<Allocations> = const { Cell::new(Allocations {
        alloc_calls: 0,
        alloc_zeroed_calls: 0,
        realloc_calls: 0,
        dealloc_calls: 0,
        allocated_bytes: 0,
        reallocated_bytes: 0,
        deallocated_bytes: 0,
    }) };
}

fn counting_is_active() -> bool {
    ACTIVE_DEPTH
        .try_with(|depth| depth.get() != 0)
        .unwrap_or(false)
}

fn update_counts(update: impl FnOnce(&mut Allocations)) {
    if !counting_is_active() {
        return;
    }
    let _unavailable_during_thread_teardown = COUNTS.try_with(|counts| {
        let mut next = counts.get();
        update(&mut next);
        counts.set(next);
    });
}

fn snapshot() -> Allocations {
    COUNTS.try_with(Cell::get).unwrap_or_default()
}

/// A [`System`] allocator that counts operations only inside [`AllocScope`].
pub struct CountingAllocator;

// SAFETY: every method delegates the exact pointer/layout operation unchanged
// to `System`. The side-channel bookkeeping touches only const-initialised
// thread-local `Cell`s, owns no allocated memory, and never changes allocator
// results or layout requirements.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies the `GlobalAlloc::alloc` layout contract,
        // and this method forwards it unchanged to `System`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            update_counts(|counts| {
                counts.alloc_calls = counts.alloc_calls.saturating_add(1);
                counts.allocated_bytes = counts
                    .allocated_bytes
                    .saturating_add(u64::try_from(layout.size()).unwrap_or(u64::MAX));
            });
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        update_counts(|counts| {
            counts.dealloc_calls = counts.dealloc_calls.saturating_add(1);
            counts.deallocated_bytes = counts
                .deallocated_bytes
                .saturating_add(u64::try_from(layout.size()).unwrap_or(u64::MAX));
        });
        // SAFETY: the caller guarantees `pointer` and `layout` describe a live
        // allocation from this allocator; it was obtained from `System`.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies the `GlobalAlloc::alloc_zeroed` layout
        // contract, and this method forwards it unchanged to `System`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            update_counts(|counts| {
                counts.alloc_zeroed_calls = counts.alloc_zeroed_calls.saturating_add(1);
                counts.allocated_bytes = counts
                    .allocated_bytes
                    .saturating_add(u64::try_from(layout.size()).unwrap_or(u64::MAX));
            });
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller guarantees `pointer` and `layout` identify a live
        // `System` allocation and `new_size` satisfies `GlobalAlloc::realloc`.
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            update_counts(|counts| {
                counts.realloc_calls = counts.realloc_calls.saturating_add(1);
                counts.reallocated_bytes = counts
                    .reallocated_bytes
                    .saturating_add(u64::try_from(new_size).unwrap_or(u64::MAX));
            });
        }
        new_pointer
    }
}

/// Installs one [`CountingAllocator`] in the invoking test binary.
#[macro_export]
macro_rules! install {
    () => {
        #[global_allocator]
        static TOLLGATE_COUNTING_ALLOCATOR: $crate::CountingAllocator = $crate::CountingAllocator;
    };
}

/// Append one JSON line when [`REPORT_ENV`] names a report file.
///
/// This is a macro so the optional environment routing does not add a
/// function to the mutation surface: [`append_report`] owns the behavior and
/// has a direct self-witness.
#[macro_export]
macro_rules! record_if_requested {
    ($scope:expr, $attribution:expr, $allocations:expr) => {{
        match ::std::env::var_os($crate::REPORT_ENV) {
            Some(path) => $crate::append_report(path, $scope, $attribution, $allocations),
            None => ::std::result::Result::Ok(()),
        }
    }};
}

struct ActiveGuard;

impl ActiveGuard {
    fn enter() -> Self {
        ACTIVE_DEPTH.with(|depth| {
            depth.set(
                depth
                    .get()
                    .checked_add(1)
                    .expect("allocation scope nesting overflow"),
            );
        });
        Self
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE_DEPTH.with(|depth| {
            depth.set(
                depth
                    .get()
                    .checked_sub(1)
                    .expect("allocation scope nesting underflow"),
            );
        });
    }
}

/// Current-thread allocation scopes.
pub struct AllocScope;

impl AllocScope {
    /// Run `operation`, returning its value and attributed heap operations.
    /// Nested scopes compose: an outer result includes its inner scopes.
    pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, Allocations) {
        let before = snapshot();
        let guard = ActiveGuard::enter();
        let output = operation();
        drop(guard);
        let allocations = snapshot().difference(before);
        (output, allocations)
    }

    /// Run `operation` and fail unless it allocated and reallocated nothing.
    pub fn assert_zero<T>(operation: impl FnOnce() -> T) -> T {
        let (output, allocations) = Self::measure(operation);
        assert!(
            allocations.is_allocation_free(),
            "expected zero allocations, observed {allocations:?}"
        );
        output
    }
}

#[derive(Serialize)]
struct ReportRow<'a> {
    scope: &'a str,
    attribution: &'a str,
    allocations: Allocations,
}

/// Append one encoded report row to `path`.
///
/// Call this only after the measured scope. The script serialises test targets
/// and each row is encoded into one buffer before the append write.
pub fn append_report(
    path: impl AsRef<Path>,
    scope: &str,
    attribution: &str,
    allocations: Allocations,
) -> io::Result<()> {
    let mut encoded = serde_json::to_vec(&ReportRow {
        scope,
        attribution,
        allocations,
    })?;
    encoded.push(b'\n');
    let mut report = OpenOptions::new().create(true).append(true).open(path)?;
    report.write_all(&encoded)
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout};
    use std::hint::black_box;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{AllocScope, Allocations, CountingAllocator, append_report, counting_is_active};

    crate::install!();

    #[test]
    fn a_box_inside_the_scope_is_counted() {
        let (_, allocations) = AllocScope::measure(|| {
            let value = Box::new(black_box(7_u64));
            black_box(&*value);
            drop(value);
        });
        assert_eq!(allocations.alloc_calls, 1);
        assert_eq!(allocations.dealloc_calls, 1);
        assert!(!allocations.is_allocation_free());
    }

    #[test]
    fn alloc_zeroed_and_realloc_are_both_visible() {
        let (_, zeroed) = AllocScope::measure(|| {
            let layout = Layout::from_size_align(64, 8).unwrap();
            // SAFETY: `layout` is nonzero and valid; the returned allocation
            // is released through the same allocator and layout.
            let pointer = unsafe { CountingAllocator.alloc_zeroed(layout) };
            assert!(!pointer.is_null());
            // SAFETY: `pointer` came from the allocator with this layout.
            unsafe { CountingAllocator.dealloc(pointer, layout) };
        });
        assert_eq!(zeroed.alloc_zeroed_calls, 1);

        #[allow(
            clippy::vec_init_then_push,
            reason = "the second push is the realloc witness"
        )]
        let (_, grown) = AllocScope::measure(|| {
            let mut values = Vec::<u64>::with_capacity(1);
            values.push(1);
            values.push(2);
            black_box(values);
        });
        assert!(grown.realloc_calls >= 1, "observed {grown:?}");
    }

    #[test]
    fn a_pure_scope_is_zero_and_outside_work_is_excluded() {
        let value = Box::new(black_box(11_u64));
        AllocScope::assert_zero(|| black_box(&*value));
        drop(value);
    }

    #[test]
    fn another_threads_allocations_are_excluded() {
        let start = Arc::new(AtomicBool::new(false));
        let finish = Arc::new(AtomicBool::new(false));
        let worker = {
            let start = Arc::clone(&start);
            let finish = Arc::clone(&finish);
            std::thread::spawn(move || {
                while !start.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                let value = Box::new(black_box(13_u64));
                black_box(&*value);
                drop(value);
                finish.store(true, Ordering::Release);
            })
        };
        AllocScope::assert_zero(|| {
            start.store(true, Ordering::Release);
            while !finish.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
        });
        worker.join().unwrap();
    }

    #[test]
    fn nested_scopes_compose_and_panics_restore_the_depth() {
        assert!(!counting_is_active());
        let (_, outer) = AllocScope::measure(|| {
            assert!(counting_is_active());
            let (_, inner) = AllocScope::measure(|| {
                assert!(counting_is_active());
                black_box(Box::new(17_u64));
            });
            assert_eq!(inner.alloc_calls, 1);
            black_box(Box::new(19_u64));
        });
        assert_eq!(outer.alloc_calls, 2);

        let panicked = catch_unwind(AssertUnwindSafe(|| {
            AllocScope::measure(|| panic!("sentinel"));
        }));
        assert!(panicked.is_err());
        assert!(!counting_is_active());
        AllocScope::assert_zero(|| black_box(23_u64));
        assert!(!counting_is_active());
    }

    #[test]
    fn report_rows_are_appended_as_valid_json_lines() {
        let path = std::env::temp_dir().join(format!(
            "tollgate-alloc-count-{}-report.json",
            std::process::id()
        ));
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove stale report: {error}"),
        }
        append_report(
            &path,
            "one",
            "tollgate",
            Allocations {
                alloc_calls: 1,
                allocated_bytes: 8,
                ..Allocations::default()
            },
        )
        .unwrap();
        append_report(&path, "two", "caller", Allocations::default()).unwrap();

        let report = std::fs::read_to_string(&path).unwrap();
        let rows = report.lines().collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(rows[0]).unwrap()["scope"],
            "one"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(rows[1]).unwrap()["scope"],
            "two"
        );
        std::fs::remove_file(path).unwrap();
    }
}
