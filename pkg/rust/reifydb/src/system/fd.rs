// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Process file-descriptor limit management.

/// Raise the soft `RLIMIT_NOFILE` (open file descriptors) limit to the hard
/// limit for the current process.
///
/// Each accepted connection (HTTP/WS/gRPC) and each outbound socket consumes
/// one file descriptor. The default soft limit (often 1024) is exhausted under
/// concurrent load and surfaces as `accept error: Too many open files (os error
/// 24)`. Raising the soft limit to the hard limit gives the process the full
/// headroom the OS already permits, without needing root.
///
/// Idempotent and safe to call more than once. Never panics: on failure it logs
/// a warning and leaves the limit unchanged. No-op on non-unix targets.
#[cfg(unix)]
pub fn raise_fd_limit() {
	use std::io::Error;

	use libc::{RLIMIT_NOFILE, getrlimit, rlimit, setrlimit};
	use tracing::{info, warn};

	// SAFETY: `getrlimit`/`setrlimit` read/write only the provided `rlimit`
	// struct for a valid resource id. The struct is fully initialized before
	// it is read.
	unsafe {
		let mut limit = rlimit {
			rlim_cur: 0,
			rlim_max: 0,
		};
		if getrlimit(RLIMIT_NOFILE, &mut limit) != 0 {
			warn!("failed to read RLIMIT_NOFILE: {}", Error::last_os_error());
			return;
		}

		if limit.rlim_cur >= limit.rlim_max {
			info!("RLIMIT_NOFILE soft limit already at hard limit ({})", limit.rlim_max);
			return;
		}

		let previous = limit.rlim_cur;
		limit.rlim_cur = limit.rlim_max;
		if setrlimit(RLIMIT_NOFILE, &limit) != 0 {
			warn!(
				"failed to raise RLIMIT_NOFILE from {} to {}: {}",
				previous,
				limit.rlim_max,
				Error::last_os_error()
			);
			return;
		}

		info!("raised RLIMIT_NOFILE soft limit from {} to {}", previous, limit.rlim_max);
	}
}

/// No-op on non-unix targets, which do not expose `RLIMIT_NOFILE`.
#[cfg(not(unix))]
pub fn raise_fd_limit() {}

#[cfg(all(test, unix))]
mod tests {
	use libc::{RLIMIT_NOFILE, getrlimit, rlimit, setrlimit};

	use super::*;

	#[test]
	fn raises_soft_limit_to_hard_and_is_idempotent() {
		// SAFETY: single getrlimit/setrlimit calls on a fully initialized struct.
		unsafe {
			let mut original = rlimit {
				rlim_cur: 0,
				rlim_max: 0,
			};
			assert_eq!(getrlimit(RLIMIT_NOFILE, &mut original), 0);

			// Deliberately lower the soft limit so the raise has an observable effect.
			let lowered = rlimit {
				rlim_cur: 64,
				rlim_max: original.rlim_max,
			};
			assert_eq!(setrlimit(RLIMIT_NOFILE, &lowered), 0);

			raise_fd_limit();

			let mut after = rlimit {
				rlim_cur: 0,
				rlim_max: 0,
			};
			assert_eq!(getrlimit(RLIMIT_NOFILE, &mut after), 0);
			assert_eq!(after.rlim_cur, after.rlim_max, "soft limit should be raised to hard limit");

			// Calling again must not lower the limit or panic.
			raise_fd_limit();
			let mut again = rlimit {
				rlim_cur: 0,
				rlim_max: 0,
			};
			assert_eq!(getrlimit(RLIMIT_NOFILE, &mut again), 0);
			assert_eq!(again.rlim_cur, after.rlim_cur);
		}
	}
}
