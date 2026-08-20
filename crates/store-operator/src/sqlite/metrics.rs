// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, atomic::Ordering};

use reifydb_core::metrics::{collect::MetricsCollector, sample::MetricsSample};
use reifydb_sqlite::memory::sweep_connection_cache;
use reifydb_value::{byte_size::ByteSize, count::Count};
use rusqlite::Connection;

use crate::{sqlite::SqliteOperatorStorage, tier::persistent::OperatorPageCacheMetrics};

const SQLITE_SCOPE: &str = "sqlite::operator";

impl SqliteOperatorStorage {
	pub fn page_cache_metrics(&self) -> OperatorPageCacheMetrics {
		let mut used = 0u64;
		let mut sampled = 0u64;
		let mut sweep = |conn: &Connection| {
			let swept = sweep_connection_cache(conn);
			self.inner.cache_hits.fetch_add(swept.hits.as_u64(), Ordering::Relaxed);
			self.inner.cache_misses.fetch_add(swept.misses.as_u64(), Ordering::Relaxed);
			used += swept.used.as_bytes();
			sampled += 1;
		};
		if let Some(guard) = self.inner.conn.try_lock()
			&& let Some(conn) = guard.as_ref()
		{
			sweep(conn);
		}
		for slot in &self.inner.readers.conns {
			if let Some(guard) = slot.try_lock()
				&& let Some(conn) = guard.as_ref()
			{
				sweep(conn);
			}
		}
		OperatorPageCacheMetrics {
			used: ByteSize::from_bytes(used),
			hits: Count::new(self.inner.cache_hits.load(Ordering::Relaxed)),
			misses: Count::new(self.inner.cache_misses.load(Ordering::Relaxed)),
			connections_sampled: Count::new(sampled),
			connections_total: Count::new(1 + self.inner.readers.conns.len() as u64),
		}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		vec![Arc::new(OperatorPageCacheCollector {
			store: self.clone(),
		})]
	}
}

struct OperatorPageCacheCollector {
	store: SqliteOperatorStorage,
}

impl MetricsCollector for OperatorPageCacheCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let metrics = self.store.page_cache_metrics();
		out.push(MetricsSample::bytes(SQLITE_SCOPE, "page_cache_used_bytes", metrics.used));
		out.push(MetricsSample::counter(SQLITE_SCOPE, "page_cache_hit_count", metrics.hits.as_u64()));
		out.push(MetricsSample::counter(SQLITE_SCOPE, "page_cache_miss_count", metrics.misses.as_u64()));
		out.push(MetricsSample::count(
			SQLITE_SCOPE,
			"page_cache_sampled_connections",
			metrics.connections_sampled.as_u64(),
		));
		out.push(MetricsSample::count(SQLITE_SCOPE, "connections_total", metrics.connections_total.as_u64()));
	}
}
