// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_testing_chaos::fd::assert_returns_every_fd;

#[test]
fn an_in_memory_database_returns_every_fd_it_opened() {
	// The chaos suites build and drop one database per iteration, thousands of times per run. A
	// lifecycle that keeps even one descriptor from its runtime (epoll, eventfd) exhausts the
	// process fd table and then surfaces as an unrelated open failure far from the real cause.
	// This file is its own test binary on purpose: the probe counts the whole process, so it is
	// only attributable when nothing else in the process is opening descriptors.
	assert_returns_every_fd("TestDb::memory", 64, || {
		drop(TestDb::memory());
	});
}

#[test]
fn a_sqlite_backed_database_returns_every_fd_it_opened() {
	// The sqlite path is the one that actually pins file descriptors: the main database plus its
	// -wal and -shm sidecars, per connection in the read pool. This is the exact shape the old
	// per-iteration probe named in its own failure message, the SQLITE_CANTOPEN mode.
	assert_returns_every_fd("TestDb::sqlite_memory", 32, || {
		drop(TestDb::sqlite_memory());
	});
}
