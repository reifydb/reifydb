// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_catalog::vtable::user::UserVTableColumn;
use reifydb_core::{
	interface::catalog::id::NamespaceId,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_value::{
	count::Count,
	fragment::Fragment,
	value::{datetime::DateTime, value_type::ValueType},
};

use crate::framework::source::MetricsSource;

/// What only the durable epoch log can see about itself.
///
/// Fed from the event bus: the log belongs to the lifecycle subsystem, and reaching into it from a metrics domain
/// would couple two subsystems that otherwise share nothing but core.
#[derive(Default)]
pub struct EpochGauge {
	durable_samples: AtomicU64,
	pruned: AtomicU64,
}

impl EpochGauge {
	pub fn record(&self, durable_samples: Count, pruned: Count) {
		self.durable_samples.store(durable_samples.as_u64(), Ordering::Relaxed);
		self.pruned.store(pruned.as_u64(), Ordering::Relaxed);
	}

	fn read(&self) -> (u64, u64) {
		(self.durable_samples.load(Ordering::Relaxed), self.pruned.load(Ordering::Relaxed))
	}
}

pub struct EpochSource {
	engine: StandardEngine,
	gauge: Arc<EpochGauge>,
}

impl EpochSource {
	pub fn new(engine: StandardEngine, gauge: Arc<EpochGauge>) -> Self {
		Self {
			engine,
			gauge,
		}
	}
}

impl MetricsSource for EpochSource {
	fn namespace(&self) -> NamespaceId {
		NamespaceId::SYSTEM_METRICS_EPOCH
	}

	fn columns(&self) -> Vec<UserVTableColumn> {
		vec![
			UserVTableColumn::new("ts", ValueType::DateTime),
			UserVTableColumn::new("samples", ValueType::Uint8),
			UserVTableColumn::new("durable_samples", ValueType::Uint8),
			UserVTableColumn::new("pruned", ValueType::Uint8),
			UserVTableColumn::new("coverage", ValueType::Duration),
			UserVTableColumn::new("guaranteed_coverage", ValueType::Duration),
			UserVTableColumn::new("floor_none_returns", ValueType::Uint8),
		]
	}

	fn collect(&self, now: DateTime) -> Columns {
		let epoch = self.engine.version_epoch();
		let stats = epoch.stats();
		let guaranteed = epoch.retention().guaranteed_coverage();
		let (durable, pruned) = self.gauge.read();

		let mut ts = ColumnBuffer::datetime_with_capacity(1);
		let mut samples = ColumnBuffer::uint8_with_capacity(1);
		let mut durable_samples = ColumnBuffer::uint8_with_capacity(1);
		let mut pruned_samples = ColumnBuffer::uint8_with_capacity(1);
		let mut coverage = ColumnBuffer::duration_with_capacity(1);
		let mut guaranteed_coverage = ColumnBuffer::duration_with_capacity(1);
		let mut floor_none_returns = ColumnBuffer::uint8_with_capacity(1);

		ts.push(now);
		samples.push(stats.samples as u64);
		durable_samples.push(durable);
		pruned_samples.push(pruned);
		coverage.push(stats.coverage.to_duration());
		guaranteed_coverage.push(guaranteed.to_duration());
		floor_none_returns.push(stats.floor_none_returns);

		Columns::new(vec![
			ColumnWithName::new(Fragment::internal("ts"), ts),
			ColumnWithName::new(Fragment::internal("samples"), samples),
			ColumnWithName::new(Fragment::internal("durable_samples"), durable_samples),
			ColumnWithName::new(Fragment::internal("pruned"), pruned_samples),
			ColumnWithName::new(Fragment::internal("coverage"), coverage),
			ColumnWithName::new(Fragment::internal("guaranteed_coverage"), guaranteed_coverage),
			ColumnWithName::new(Fragment::internal("floor_none_returns"), floor_none_returns),
		])
	}
}

pub fn epoch_sources(engine: &StandardEngine, gauge: Arc<EpochGauge>) -> Vec<Arc<dyn MetricsSource>> {
	vec![Arc::new(EpochSource::new(engine.clone(), gauge)) as Arc<dyn MetricsSource>]
}
