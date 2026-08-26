// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod pool;

use std::sync::atomic::{AtomicU64, Ordering};

use pool::ReadPool;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	memory::sweep_connection_cache,
	pragma,
};
use reifydb_value::{byte_size::ByteSize, count::Count, value::duration::Duration};
use rusqlite::Connection;

use crate::metrics::PageCacheMetrics;

const BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(200);

pub struct OpenMessages {
	pub connect: &'static str,
	pub pragmas: &'static str,
	pub busy_timeout: &'static str,
	pub read_connect: &'static str,
	pub read_pragmas: &'static str,
	pub read_busy_timeout: &'static str,
}

pub fn open(config: &SqliteConfig, file_name: &str, messages: &OpenMessages) -> (Connection, ReadPool) {
	let path = resolve_db_path(config.path.clone(), file_name);
	let flags = convert_flags(&config.flags);

	let conn = connect(&path, flags).expect(messages.connect);
	pragma::apply(&conn, config).expect(messages.pragmas);
	conn.busy_timeout(BUSY_TIMEOUT.to_std()).expect(messages.busy_timeout);

	let pool_size = config.read_pool_size.max(1) as usize;
	let mut readers = Vec::with_capacity(pool_size);
	for _ in 0..pool_size {
		let reader = connect(&path, flags).expect(messages.read_connect);
		pragma::apply_read_only(&reader, config).expect(messages.read_pragmas);
		reader.busy_timeout(BUSY_TIMEOUT.to_std()).expect(messages.read_busy_timeout);
		readers.push(reader);
	}

	(conn, ReadPool::new(readers))
}

pub fn page_cache_metrics(
	conn: &Mutex<Option<Connection>>,
	readers: &ReadPool,
	hits: &AtomicU64,
	misses: &AtomicU64,
) -> PageCacheMetrics {
	let mut used = 0u64;
	let mut sampled = 0u64;
	let mut sweep = |conn: &Connection| {
		let swept = sweep_connection_cache(conn);
		hits.fetch_add(swept.hits.as_u64(), Ordering::Relaxed);
		misses.fetch_add(swept.misses.as_u64(), Ordering::Relaxed);
		used += swept.used.as_bytes();
		sampled += 1;
	};
	if let Some(guard) = conn.try_lock()
		&& let Some(conn) = guard.as_ref()
	{
		sweep(conn);
	}
	for slot in &readers.conns {
		if let Some(guard) = slot.try_lock()
			&& let Some(conn) = guard.as_ref()
		{
			sweep(conn);
		}
	}
	PageCacheMetrics {
		used: ByteSize::from_bytes(used),
		hits: Count::new(hits.load(Ordering::Relaxed)),
		misses: Count::new(misses.load(Ordering::Relaxed)),
		connections_sampled: Count::new(sampled),
		connections_total: Count::new(1 + readers.conns.len() as u64),
	}
}
