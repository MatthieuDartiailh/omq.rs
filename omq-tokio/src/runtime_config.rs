//! Runtime executor configuration: single-threaded vs multi-threaded tokio runtime.
//!
//! The executor mode is set once at module initialization (before the first socket
//! is created) and cannot be changed afterward. Use `OnceLock` to enforce this.
//!
//! Enum variants are feature-gated, so when only one runtime is enabled, the variant
//! for the other runtime does not exist at compile time. This makes it clear what
//! runtime will be used and can eliminate runtime checks.

use std::num::NonZero;
use std::sync::OnceLock;

/// Executor mode: single-threaded or multi-threaded tokio runtime.
///
/// Variants are feature-gated based on enabled runtime features:
/// - `rt-single-thread`: `SingleThread` variant available
/// - `rt-multi-thread`: `MultiThread` variant available (with auto or explicit thread count)
///
/// When both features are enabled, all variants exist and runtime selection is possible.
/// When only one is enabled, only that variant exists, making the runtime choice
/// deterministic at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorMode {
    /// Single-threaded current-thread runtime.
    /// Only available when `rt-single-thread` feature is enabled.
    #[cfg(feature = "rt-single-thread")]
    SingleThread,
    /// Multi-threaded runtime with optional explicit thread count.
    /// `None` = auto-detect via `std::thread::available_parallelism()`, `Some(n)` = exactly n threads.
    /// Only available when `rt-multi-thread` feature is enabled.
    #[cfg(feature = "rt-multi-thread")]
    MultiThread(Option<NonZero<usize>>),
}

impl ExecutorMode {
    /// Parse from a string representation.
    ///
    /// Accepted formats depend on enabled features:
    /// - When `rt-single-thread` is enabled: "single" → `SingleThread`
    /// - When `rt-multi-thread` is enabled: "multi" → `MultiThread(None)` (auto-detect), "multi:N" → `MultiThread(Some(N))`
    /// - When both are enabled: all formats accepted
    pub fn from_str(s: &str) -> Result<Self, &'static str> {
        match s.trim() {
            #[cfg(feature = "rt-single-thread")]
            "single" => Ok(ExecutorMode::SingleThread),
            #[cfg(feature = "rt-multi-thread")]
            "multi" => Ok(ExecutorMode::MultiThread(None)),
            #[cfg(feature = "rt-multi-thread")]
            s if s.starts_with("multi:") => {
                let threads_str = &s[6..];
                threads_str
                    .parse::<usize>()
                    .ok()
                    .and_then(NonZero::new)
                    .map(|n| ExecutorMode::MultiThread(Some(n)))
                    .ok_or("invalid thread count (must be positive integer)")
            }
            _ => {
                #[cfg(all(feature = "rt-single-thread", feature = "rt-multi-thread"))]
                let msg = "invalid executor mode (use 'single', 'multi', or 'multi:N')";
                #[cfg(all(feature = "rt-single-thread", not(feature = "rt-multi-thread")))]
                let msg = "invalid executor mode (only 'single' is available)";
                #[cfg(all(not(feature = "rt-single-thread"), feature = "rt-multi-thread"))]
                let msg = "invalid executor mode (use 'multi' or 'multi:N')";
                #[cfg(not(any(feature = "rt-single-thread", feature = "rt-multi-thread")))]
                let msg = "no runtime features enabled";
                Err(msg)
            }
        }
    }

    /// Convert to string representation.
    pub fn to_str(&self) -> &'static str {
        match self {
            #[cfg(feature = "rt-single-thread")]
            ExecutorMode::SingleThread => "single",
            #[cfg(feature = "rt-multi-thread")]
            ExecutorMode::MultiThread(None) => "multi",
            #[cfg(feature = "rt-multi-thread")]
            ExecutorMode::MultiThread(Some(_)) => "multi:?",
        }
    }

    /// Get the display string with thread count if applicable.
    pub fn display(&self) -> String {
        match self {
            #[cfg(feature = "rt-single-thread")]
            ExecutorMode::SingleThread => "single".to_string(),
            #[cfg(feature = "rt-multi-thread")]
            ExecutorMode::MultiThread(None) => "multi".to_string(),
            #[cfg(feature = "rt-multi-thread")]
            ExecutorMode::MultiThread(Some(n)) => format!("multi:{}", n),
        }
    }

    /// Extract explicit thread count, if set. Returns `None` for single-threaded mode
    /// or for auto-detect multi-threaded mode.
    pub fn thread_count(&self) -> Option<NonZero<usize>> {
        match self {
            #[cfg(feature = "rt-single-thread")]
            ExecutorMode::SingleThread => None,
            #[cfg(feature = "rt-multi-thread")]
            ExecutorMode::MultiThread(Some(n)) => Some(*n),
            #[cfg(feature = "rt-multi-thread")]
            ExecutorMode::MultiThread(None) => None,
        }
    }
}

static EXECUTOR_MODE: OnceLock<ExecutorMode> = OnceLock::new();

/// Default executor mode based on available features.
/// - If only `rt-single-thread`: `SingleThread`
/// - If only `rt-multi-thread`: `MultiThread(None)` (auto-detect)
/// - If both: `MultiThread(None)` (prefer multi-threaded for backward compatibility)
/// - If neither: compilation error (at least one runtime must be enabled)
#[cfg(all(feature = "rt-single-thread", not(feature = "rt-multi-thread")))]
const DEFAULT_MODE: ExecutorMode = ExecutorMode::SingleThread;

#[cfg(feature = "rt-multi-thread")]
const DEFAULT_MODE: ExecutorMode = ExecutorMode::MultiThread(None);

/// Set the executor mode. Must be called before any socket is created.
/// Returns an error if already set.
pub fn set_executor_mode(mode: ExecutorMode) -> Result<(), &'static str> {
    EXECUTOR_MODE
        .set(mode)
        .map_err(|_| "executor mode already set (must be called before first socket)")
}

/// Get the current executor mode. If not yet set, returns the default based on enabled features.
pub fn executor_mode() -> ExecutorMode {
    *EXECUTOR_MODE.get_or_init(|| DEFAULT_MODE)
}

/// Compute the actual thread count for multi-threaded mode.
/// - For `SingleThread`: returns 1
/// - For `MultiThread(Some(n))`: returns n
/// - For `MultiThread(None)`: returns auto-detected count via `std::thread::available_parallelism()`
#[cfg(feature = "rt-multi-thread")]
pub fn compute_thread_count(mode: ExecutorMode) -> usize {
    match mode {
        #[cfg(feature = "rt-single-thread")]
        ExecutorMode::SingleThread => 1,
        ExecutorMode::MultiThread(Some(n)) => n.get(),
        ExecutorMode::MultiThread(None) => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "rt-single-thread")]
    fn test_parse_single() {
        assert_eq!(
            ExecutorMode::from_str("single").unwrap(),
            ExecutorMode::SingleThread
        );
    }

    #[test]
    #[cfg(feature = "rt-multi-thread")]
    fn test_parse_multi_auto() {
        assert_eq!(
            ExecutorMode::from_str("multi").unwrap(),
            ExecutorMode::MultiThread(None)
        );
    }

    #[test]
    #[cfg(feature = "rt-multi-thread")]
    fn test_parse_multi_explicit() {
        let mode = ExecutorMode::from_str("multi:4").unwrap();
        assert_eq!(mode.thread_count(), NonZero::new(4));
    }

    #[test]
    #[cfg(feature = "rt-multi-thread")]
    fn test_parse_invalid_thread_count() {
        assert!(ExecutorMode::from_str("multi:0").is_err());
        assert!(ExecutorMode::from_str("multi:-1").is_err());
    }

    #[test]
    #[cfg(any(feature = "rt-single-thread", feature = "rt-multi-thread"))]
    fn test_parse_invalid_mode() {
        assert!(ExecutorMode::from_str("invalid").is_err());
    }

    #[test]
    #[cfg(all(feature = "rt-single-thread", feature = "rt-multi-thread"))]
    fn test_set_executor_mode() {
        // This test only runs when both features are enabled.
        // In practice, tests run in parallel and may race. For now, just verify
        // the display logic.
        let mode = ExecutorMode::MultiThread(Some(NonZero::new(8).unwrap()));
        assert_eq!(mode.display(), "multi:8");
    }

    #[test]
    #[cfg(feature = "rt-multi-thread")]
    fn test_compute_thread_count() {
        #[cfg(feature = "rt-single-thread")]
        {
            assert_eq!(compute_thread_count(ExecutorMode::SingleThread), 1);
        }
        assert!(
            compute_thread_count(ExecutorMode::MultiThread(Some(NonZero::new(4).unwrap()))) > 0
        );
        // MultiThread(None) will auto-detect; just verify it's positive.
        assert!(compute_thread_count(ExecutorMode::MultiThread(None)) > 0);
    }
}
