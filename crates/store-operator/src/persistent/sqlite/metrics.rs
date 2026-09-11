// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::metrics::{collect::MetricsCollector, sample::MetricsSample};
use reifydb_store::{metrics::PageCacheMetrics, sqlite::page_cache_metrics};

use crate::persistent::sqlite::{SqlitePersistent, StoreInner};

const SQLITE_SCOPE: &str = "sqlite::operator";

impl StoreInner {
	fn page_cache_metrics(&self) -> PageCacheMetrics {
		page_cache_metrics(&self.conn, &self.readers, &self.cache_hits, &self.cache_misses)
	}
}

impl SqlitePersistent {
	pub fn page_cache_metrics(&self) -> PageCacheMetrics {
		self.inner.page_cache_metrics()
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		vec![self.collector.clone()]
	}
}

pub(super) fn page_cache_collector(inner: Arc<StoreInner>) -> Arc<dyn MetricsCollector> {
	Arc::new(OperatorPageCacheCollector {
		inner,
	})
}

struct OperatorPageCacheCollector {
	inner: Arc<StoreInner>,
}

impl MetricsCollector for OperatorPageCacheCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let metrics = self.inner.page_cache_metrics();
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
