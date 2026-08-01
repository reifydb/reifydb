// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

/// The default soft `RLIMIT_NOFILE` (often 1024) is exhausted by concurrent connections and
/// surfaces as `accept error: Too many open files (os error 24)`; the hard limit is reachable
/// without root. Idempotent, and never panics: on failure it warns and leaves the limit unchanged.
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
