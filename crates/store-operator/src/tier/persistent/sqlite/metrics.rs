// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::metrics::{collect::MetricsCollector, sample::MetricsSample};
use reifydb_store::{metrics::PageCacheMetrics, sqlite::page_cache_metrics};

use crate::tier::persistent::sqlite::SqliteOperatorStorage;

const SQLITE_SCOPE: &str = "sqlite::operator";

impl SqliteOperatorStorage {
	pub fn page_cache_metrics(&self) -> PageCacheMetrics {
		page_cache_metrics(
			&self.inner.conn,
			&self.inner.readers,
			&self.inner.cache_hits,
			&self.inner.cache_misses,
		)
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
