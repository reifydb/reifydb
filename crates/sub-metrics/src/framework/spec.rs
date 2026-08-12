// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::vtable::user::UserVTableColumn;
use reifydb_core::{interface::catalog::id::NamespaceId, metrics::sample::MetricKind};
use reifydb_value::value::value_type::ValueType;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetricsDomain {
	RuntimeMemory,
	RuntimeWatermarks,
	RuntimeOperators,
	ReadBuffer,
	Instruments,
	Epoch,
	Lifecycle,
	Storage,
	Cdc,
	ProfilerSpans,
	FlowState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Surface {
	Current,
	Total,
}

impl Surface {
	pub fn table_name(&self) -> &'static str {
		match self {
			Surface::Current => "current",
			Surface::Total => "total",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomainShape {
	Long,
	Wide,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushKind {
	Census,
	Update,
}

#[derive(Clone, Debug)]
pub struct DimensionSpec {
	pub name: &'static str,
	pub data_type: ValueType,
	pub optional: bool,
}

impl DimensionSpec {
	pub fn buffer_type(&self) -> ValueType {
		if self.optional {
			ValueType::Option(Box::new(self.data_type.clone()))
		} else {
			self.data_type.clone()
		}
	}
}

#[derive(Clone, Debug)]
pub struct MeasureSpec {
	pub name: &'static str,
	pub data_type: ValueType,
	pub kind: MetricKind,
	pub optional: bool,
}

impl MeasureSpec {
	pub fn buffer_type(&self) -> ValueType {
		if self.optional {
			ValueType::Option(Box::new(self.data_type.clone()))
		} else {
			self.data_type.clone()
		}
	}
}

#[derive(Clone, Debug)]
pub struct DomainSpec {
	pub domain: MetricsDomain,
	pub namespace: NamespaceId,
	pub shape: DomainShape,
	pub dimensions: Vec<DimensionSpec>,
	pub measures: Vec<MeasureSpec>,
	pub has_total: bool,
}

impl DomainSpec {
	pub fn columns(&self, surface: Surface) -> Vec<UserVTableColumn> {
		match self.shape {
			DomainShape::Long => long_columns(),
			DomainShape::Wide => self.wide_columns(surface),
		}
	}

	pub fn surface_measures(&self, surface: Surface) -> Vec<&MeasureSpec> {
		self.measures.iter().filter(|m| surface == Surface::Current || m.kind != MetricKind::Level).collect()
	}

	fn wide_columns(&self, surface: Surface) -> Vec<UserVTableColumn> {
		let mut columns = vec![UserVTableColumn::new("ts", ValueType::DateTime)];
		for dimension in &self.dimensions {
			let mut column = UserVTableColumn::new(dimension.name, dimension.data_type.clone());
			column.undefined = dimension.optional;
			columns.push(column);
		}
		for measure in self.surface_measures(surface) {
			let column_kind = if surface == Surface::Current && measure.kind == MetricKind::Counter {
				MetricKind::Delta
			} else {
				measure.kind
			};
			let mut column =
				UserVTableColumn::measure(measure.name, measure.data_type.clone(), column_kind);
			column.undefined = measure.optional;
			columns.push(column);
		}
		columns
	}
}

fn long_columns() -> Vec<UserVTableColumn> {
	vec![
		UserVTableColumn::new("ts", ValueType::DateTime),
		UserVTableColumn::new("scope", ValueType::Utf8),
		UserVTableColumn::new("metric", ValueType::Utf8),
		UserVTableColumn::measure("value", ValueType::Float8, MetricKind::Level),
		UserVTableColumn::new("unit", ValueType::Utf8),
		UserVTableColumn::new("kind", ValueType::Utf8),
	]
}

fn dim(name: &'static str, data_type: ValueType) -> DimensionSpec {
	DimensionSpec {
		name,
		data_type,
		optional: false,
	}
}

fn dim_optional(name: &'static str, data_type: ValueType) -> DimensionSpec {
	DimensionSpec {
		name,
		data_type,
		optional: true,
	}
}

fn level(name: &'static str, data_type: ValueType) -> MeasureSpec {
	MeasureSpec {
		name,
		data_type,
		kind: MetricKind::Level,
		optional: false,
	}
}

fn level_optional(name: &'static str, data_type: ValueType) -> MeasureSpec {
	MeasureSpec {
		name,
		data_type,
		kind: MetricKind::Level,
		optional: true,
	}
}

fn counter(name: &'static str, data_type: ValueType) -> MeasureSpec {
	MeasureSpec {
		name,
		data_type,
		kind: MetricKind::Counter,
		optional: false,
	}
}

fn distribution(name: &'static str, data_type: ValueType) -> MeasureSpec {
	MeasureSpec {
		name,
		data_type,
		kind: MetricKind::Distribution,
		optional: false,
	}
}

fn long_spec(domain: MetricsDomain, namespace: NamespaceId, has_total: bool) -> DomainSpec {
	DomainSpec {
		domain,
		namespace,
		shape: DomainShape::Long,
		dimensions: Vec::new(),
		measures: Vec::new(),
		has_total,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::metrics::sample::MetricKind;

	use super::{MetricsDomain, Surface};

	#[test]
	fn no_current_surface_declares_a_counter_column() {
		// The enforced rule of the redesign: ::current holds levels, deltas and distributions
		// only; a Counter column there is the summed-up-in-current disease coming back.
		for domain in MetricsDomain::ALL {
			let spec = domain.spec();
			for column in spec.columns(Surface::Current) {
				assert!(
					column.kind != MetricKind::Counter,
					"{:?} declares Counter column '{}' in its ::current surface",
					domain,
					column.name
				);
			}
		}
	}

	#[test]
	fn counter_measures_publish_as_delta_in_current_and_counter_in_total() {
		// The same measure name serves both surfaces; only the kind differs, which is what
		// makes the boot-time column check enforceable.
		let spec = MetricsDomain::ReadBuffer.spec();
		let current = spec.columns(Surface::Current);
		let in_current = current.iter().find(|c| c.name == "warms_started").expect("column");
		assert_eq!(in_current.kind, MetricKind::Delta);

		let total = spec.columns(Surface::Total);
		let in_total = total.iter().find(|c| c.name == "warms_started").expect("column");
		assert_eq!(in_total.kind, MetricKind::Counter);
		assert!(!total.iter().any(|c| c.name == "used"), "levels must not appear in a ::total surface");
	}
}

impl MetricsDomain {
	pub const ALL: [MetricsDomain; 11] = [
		MetricsDomain::RuntimeMemory,
		MetricsDomain::RuntimeWatermarks,
		MetricsDomain::RuntimeOperators,
		MetricsDomain::ReadBuffer,
		MetricsDomain::Instruments,
		MetricsDomain::Epoch,
		MetricsDomain::Lifecycle,
		MetricsDomain::Storage,
		MetricsDomain::Cdc,
		MetricsDomain::ProfilerSpans,
		MetricsDomain::FlowState,
	];

	pub fn push_kind(self) -> PushKind {
		match self {
			MetricsDomain::Storage | MetricsDomain::Cdc | MetricsDomain::FlowState => PushKind::Census,
			MetricsDomain::RuntimeMemory
			| MetricsDomain::RuntimeWatermarks
			| MetricsDomain::RuntimeOperators
			| MetricsDomain::ReadBuffer
			| MetricsDomain::Instruments
			| MetricsDomain::Epoch
			| MetricsDomain::Lifecycle
			| MetricsDomain::ProfilerSpans => PushKind::Update,
		}
	}

	pub fn snapshots_path(self) -> &'static str {
		match self {
			MetricsDomain::RuntimeMemory => "system::metrics::runtime::memory::snapshots",
			MetricsDomain::RuntimeWatermarks => "system::metrics::runtime::watermarks::snapshots",
			MetricsDomain::RuntimeOperators => "system::metrics::runtime::operators::snapshots",
			MetricsDomain::ReadBuffer => "system::metrics::read_buffer::snapshots",
			MetricsDomain::Instruments => "system::metrics::instruments::snapshots",
			MetricsDomain::Epoch => "system::metrics::epoch::snapshots",
			MetricsDomain::Lifecycle => "system::metrics::lifecycle::snapshots",
			MetricsDomain::Storage => "system::metrics::storage::snapshots",
			MetricsDomain::Cdc => "system::metrics::cdc::snapshots",
			MetricsDomain::ProfilerSpans => "system::metrics::profiler::spans::snapshots",
			MetricsDomain::FlowState => "system::metrics::flow::state::snapshots",
		}
	}

	pub fn spec(self) -> DomainSpec {
		match self {
			MetricsDomain::RuntimeMemory => {
				long_spec(self, NamespaceId::SYSTEM_METRICS_RUNTIME_MEMORY, false)
			}
			MetricsDomain::RuntimeWatermarks => {
				long_spec(self, NamespaceId::SYSTEM_METRICS_RUNTIME_WATERMARKS, false)
			}
			MetricsDomain::RuntimeOperators => {
				long_spec(self, NamespaceId::SYSTEM_METRICS_RUNTIME_OPERATORS, true)
			}
			MetricsDomain::Instruments => long_spec(self, NamespaceId::SYSTEM_METRICS_INSTRUMENTS, true),
			MetricsDomain::ReadBuffer => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_READ_BUFFER,
				shape: DomainShape::Wide,
				dimensions: vec![dim("shard", ValueType::Uint2)],
				measures: vec![
					level("used", ValueType::Uint8),
					level("limit", ValueType::Uint8),
					level("pages", ValueType::Uint8),
					level("page_cap", ValueType::Uint8),
					level("payload", ValueType::Uint8),
					level("entries", ValueType::Uint8),
					level("hot_pages", ValueType::Uint8),
					level("complete_pages", ValueType::Uint8),
					level("blocked_pages", ValueType::Uint8),
					level("warming", ValueType::Uint8),
					counter("warms_started", ValueType::Uint8),
					counter("warms_completed", ValueType::Uint8),
					counter("warms_dirty_aborted", ValueType::Uint8),
					counter("warms_aborted", ValueType::Uint8),
					counter("pages_warm_blocked", ValueType::Uint8),
					counter("pages_evicted", ValueType::Uint8),
					counter("complete_pages_invalidated", ValueType::Uint8),
					counter("point_hits", ValueType::Uint8),
					counter("previous_hits", ValueType::Uint8),
					counter("point_misses", ValueType::Uint8),
					counter("range_served", ValueType::Uint8),
					counter("range_gaps", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::Epoch => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_EPOCH,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("samples", ValueType::Uint8),
					level("durable_samples", ValueType::Uint8),
					level("coverage", ValueType::Duration),
					level("guaranteed_coverage", ValueType::Duration),
					counter("pruned", ValueType::Uint8),
					counter("floor_none_returns", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::Lifecycle => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_LIFECYCLE,
				shape: DomainShape::Wide,
				dimensions: vec![
					dim("class", ValueType::Utf8),
					dim_optional("binding", ValueType::Utf8),
				],
				measures: vec![
					level("floor_version", ValueType::Uint8),
					level("backlog_hint", ValueType::Uint8),
					level_optional("freelist_pages", ValueType::Uint8),
					level_optional("page_count", ValueType::Uint8),
					counter("work_done", ValueType::Uint8),
					counter("slices", ValueType::Uint8),
					counter("stuck_slices", ValueType::Uint8),
					counter("budget_exhausted_slices", ValueType::Uint8),
					counter("gated_slices", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::Storage => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORAGE,
				shape: DomainShape::Wide,
				dimensions: vec![
					dim("object_kind", ValueType::Utf8),
					dim("id", ValueType::Uint8),
					dim("namespace_id", ValueType::Uint8),
					dim("tier", ValueType::Utf8),
				],
				measures: vec![
					level("live_key_bytes", ValueType::Uint8),
					level("live_value_bytes", ValueType::Uint8),
					level("live_bytes", ValueType::Uint8),
					level("live_count", ValueType::Uint8),
					level("superseded_key_bytes", ValueType::Uint8),
					level("superseded_value_bytes", ValueType::Uint8),
					level("superseded_bytes", ValueType::Uint8),
					level("superseded_count", ValueType::Uint8),
					level("total_bytes", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::FlowState => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_FLOW_STATE,
				shape: DomainShape::Wide,
				dimensions: vec![
					dim("operator", ValueType::Uint8),
					dim("group", ValueType::Uint8),
					dim("keyspace", ValueType::Utf8),
					dim("phase", ValueType::Utf8),
				],
				measures: vec![
					level("keys", ValueType::Uint8),
					level("key_bytes", ValueType::Uint8),
					level("value_bytes", ValueType::Uint8),
					level("total_bytes", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::Cdc => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_CDC,
				shape: DomainShape::Wide,
				dimensions: vec![
					dim("object_kind", ValueType::Utf8),
					dim("id", ValueType::Uint8),
					dim("namespace_id", ValueType::Uint8),
				],
				measures: vec![
					level("key_bytes", ValueType::Uint8),
					level("value_bytes", ValueType::Uint8),
					level("total_bytes", ValueType::Uint8),
					level("count", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::ProfilerSpans => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROFILER_SPANS,
				shape: DomainShape::Wide,
				dimensions: vec![
					dim("category", ValueType::Utf8),
					dim("span_name", ValueType::Utf8),
					dim("dim_1", ValueType::Utf8),
					dim("dim_2", ValueType::Utf8),
				],
				measures: vec![
					counter("calls", ValueType::Uint8),
					counter("total", ValueType::Duration),
					distribution("min", ValueType::Duration),
					distribution("p50", ValueType::Duration),
					distribution("p75", ValueType::Duration),
					distribution("p90", ValueType::Duration),
					distribution("p95", ValueType::Duration),
					distribution("p98", ValueType::Duration),
					distribution("p99", ValueType::Duration),
					distribution("max", ValueType::Duration),
					counter("input_rows", ValueType::Uint8),
					counter("output_rows", ValueType::Uint8),
					counter("lock_wait", ValueType::Duration),
				],
				has_total: true,
			},
		}
	}
}
