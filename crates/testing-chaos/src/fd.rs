// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(target_os = "linux")]
use std::fs;
use std::sync::OnceLock;

use reifydb_runtime::sync::mutex::Mutex;

static PROBE: OnceLock<Mutex<()>> = OnceLock::new();

const FD_SLACK: usize = 8;

pub fn assert_returns_every_fd(label: &str, cycles: usize, mut body: impl FnMut()) {
	let _guard = PROBE.get_or_init(|| Mutex::new(())).lock();
	body();
	let before = open_fd_count();
	for _ in 0..cycles {
		body();
	}
	let after = open_fd_count();
	assert!(
		after <= before + FD_SLACK,
		"{label}: open file descriptors grew from {before} to {after} across {cycles} lifecycles (slack \
		 {FD_SLACK}); a database lifecycle is leaking fds, the SQLITE_CANTOPEN failure mode"
	);
}

#[cfg(target_os = "linux")]
fn open_fd_count() -> usize {
	fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn open_fd_count() -> usize {
	0
}
