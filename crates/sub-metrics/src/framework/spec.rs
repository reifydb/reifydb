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
	ProcProcessIo,
	ProcProcessMemory,
	ProcProcessSched,
	ProcCgroupIo,
	ProcCgroupMemory,
	ProcCgroupCpu,
	ProcCgroupPressure,
	StoreMultiCommit,
	StoreMultiRead,
	StoreMultiPersistent,
	StoreSingleCommit,
	StoreSinglePersistent,
	StoreOperatorPoint,
	StoreOperatorPointKeyspace,
	StoreOperatorRange,
	StoreOperatorRangeKeyspace,
	StoreOperatorPersistent,
	StoreCdcCommit,
	StoreCdcRead,
	StoreCdcPersistent,
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
		self.measures
			.iter()
			.filter(|m| {
				surface == Surface::Current
					|| !matches!(m.kind, MetricKind::Level | MetricKind::Cumulative)
			})
			.collect()
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

fn counter(name: &'static str, data_type: ValueType) -> MeasureSpec {
	MeasureSpec {
		name,
		data_type,
		kind: MetricKind::Counter,
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

fn cumulative(name: &'static str, data_type: ValueType) -> MeasureSpec {
	MeasureSpec {
		name,
		data_type,
		kind: MetricKind::Cumulative,
		optional: false,
	}
}

fn cumulative_optional(name: &'static str, data_type: ValueType) -> MeasureSpec {
	MeasureSpec {
		name,
		data_type,
		kind: MetricKind::Cumulative,
		optional: true,
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
		let spec = MetricsDomain::StoreMultiRead.spec();
		let current = spec.columns(Surface::Current);
		let in_current = current.iter().find(|c| c.name == "installs").expect("column");
		assert_eq!(in_current.kind, MetricKind::Delta);

		let total = spec.columns(Surface::Total);
		let in_total = total.iter().find(|c| c.name == "installs").expect("column");
		assert_eq!(in_total.kind, MetricKind::Counter);
		assert!(!total.iter().any(|c| c.name == "used"), "levels must not appear in a ::total surface");
	}
}

impl MetricsDomain {
	pub const ALL: [MetricsDomain; 30] = [
		MetricsDomain::RuntimeMemory,
		MetricsDomain::RuntimeWatermarks,
		MetricsDomain::RuntimeOperators,
		MetricsDomain::ProcProcessIo,
		MetricsDomain::ProcProcessMemory,
		MetricsDomain::ProcProcessSched,
		MetricsDomain::ProcCgroupIo,
		MetricsDomain::ProcCgroupMemory,
		MetricsDomain::ProcCgroupCpu,
		MetricsDomain::ProcCgroupPressure,
		MetricsDomain::StoreMultiCommit,
		MetricsDomain::StoreMultiRead,
		MetricsDomain::StoreMultiPersistent,
		MetricsDomain::StoreSingleCommit,
		MetricsDomain::StoreSinglePersistent,
		MetricsDomain::StoreOperatorPoint,
		MetricsDomain::StoreOperatorPointKeyspace,
		MetricsDomain::StoreOperatorRange,
		MetricsDomain::StoreOperatorRangeKeyspace,
		MetricsDomain::StoreOperatorPersistent,
		MetricsDomain::StoreCdcCommit,
		MetricsDomain::StoreCdcRead,
		MetricsDomain::StoreCdcPersistent,
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
			MetricsDomain::Storage
			| MetricsDomain::Cdc
			| MetricsDomain::FlowState
			| MetricsDomain::ProcProcessIo
			| MetricsDomain::ProcProcessMemory
			| MetricsDomain::ProcProcessSched
			| MetricsDomain::ProcCgroupIo
			| MetricsDomain::ProcCgroupMemory
			| MetricsDomain::ProcCgroupCpu
			| MetricsDomain::ProcCgroupPressure => PushKind::Census,
			MetricsDomain::RuntimeMemory
			| MetricsDomain::RuntimeWatermarks
			| MetricsDomain::RuntimeOperators
			| MetricsDomain::StoreMultiCommit
			| MetricsDomain::StoreMultiRead
			| MetricsDomain::StoreMultiPersistent
			| MetricsDomain::StoreSingleCommit
			| MetricsDomain::StoreSinglePersistent
			| MetricsDomain::StoreOperatorPoint
			| MetricsDomain::StoreOperatorPointKeyspace
			| MetricsDomain::StoreOperatorRange
			| MetricsDomain::StoreOperatorRangeKeyspace
			| MetricsDomain::StoreOperatorPersistent
			| MetricsDomain::StoreCdcCommit
			| MetricsDomain::StoreCdcRead
			| MetricsDomain::StoreCdcPersistent
			| MetricsDomain::Instruments
			| MetricsDomain::Epoch
			| MetricsDomain::Lifecycle
			| MetricsDomain::ProfilerSpans => PushKind::Update,
		}
	}

	pub fn snapshots_path(self) -> Option<&'static str> {
		match self {
			MetricsDomain::RuntimeMemory => Some("system::metrics::runtime::memory::snapshots"),
			MetricsDomain::RuntimeWatermarks => Some("system::metrics::runtime::watermarks::snapshots"),
			MetricsDomain::RuntimeOperators => Some("system::metrics::runtime::operators::snapshots"),
			MetricsDomain::Instruments => Some("system::metrics::instruments::snapshots"),
			MetricsDomain::Epoch => Some("system::metrics::epoch::snapshots"),
			MetricsDomain::Lifecycle => Some("system::metrics::lifecycle::snapshots"),
			MetricsDomain::Storage => Some("system::metrics::storage::snapshots"),
			MetricsDomain::Cdc => Some("system::metrics::cdc::snapshots"),
			MetricsDomain::ProfilerSpans => Some("system::metrics::profiler::spans::snapshots"),
			MetricsDomain::FlowState => Some("system::metrics::flow::state::snapshots"),
			MetricsDomain::ProcProcessIo
			| MetricsDomain::ProcProcessMemory
			| MetricsDomain::ProcProcessSched
			| MetricsDomain::ProcCgroupIo
			| MetricsDomain::ProcCgroupMemory
			| MetricsDomain::ProcCgroupCpu
			| MetricsDomain::ProcCgroupPressure
			| MetricsDomain::StoreMultiCommit
			| MetricsDomain::StoreMultiRead
			| MetricsDomain::StoreMultiPersistent
			| MetricsDomain::StoreSingleCommit
			| MetricsDomain::StoreSinglePersistent
			| MetricsDomain::StoreOperatorPoint
			| MetricsDomain::StoreOperatorPointKeyspace
			| MetricsDomain::StoreOperatorRange
			| MetricsDomain::StoreOperatorRangeKeyspace
			| MetricsDomain::StoreOperatorPersistent
			| MetricsDomain::StoreCdcCommit
			| MetricsDomain::StoreCdcRead
			| MetricsDomain::StoreCdcPersistent => None,
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
			MetricsDomain::ProcProcessIo => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROC_PROCESS_IO,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					cumulative("rchar", ValueType::Uint8),
					cumulative("wchar", ValueType::Uint8),
					cumulative("read_bytes", ValueType::Uint8),
					cumulative("write_bytes", ValueType::Uint8),
					cumulative("cancelled_write_bytes", ValueType::Uint8),
					cumulative("read_syscalls", ValueType::Uint8),
					cumulative("write_syscalls", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::ProcProcessMemory => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROC_PROCESS_MEMORY,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("rss_total", ValueType::Uint8),
					level("rss_anon", ValueType::Uint8),
					level("rss_file", ValueType::Uint8),
					level("rss_shmem", ValueType::Uint8),
					level("vm_size", ValueType::Uint8),
					level("vm_data", ValueType::Uint8),
					level("vm_swap", ValueType::Uint8),
					level("vm_high_water_mark", ValueType::Uint8),
					level_optional("private_dirty", ValueType::Uint8),
					level_optional("private_clean", ValueType::Uint8),
					level_optional("pss", ValueType::Uint8),
					level_optional("uss", ValueType::Uint8),
					level("threads", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::ProcProcessSched => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROC_PROCESS_SCHED,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					cumulative("minor_faults", ValueType::Uint8),
					cumulative("major_faults", ValueType::Uint8),
					cumulative("user_time", ValueType::Duration),
					cumulative("system_time", ValueType::Duration),
					cumulative("run_queue_wait", ValueType::Duration),
					cumulative("voluntary_context_switches", ValueType::Uint8),
					cumulative("involuntary_context_switches", ValueType::Uint8),
					level("open_files", ValueType::Uint8),
					level("max_open_files", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::ProcCgroupIo => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROC_CGROUP_IO,
				shape: DomainShape::Wide,
				dimensions: vec![dim("device", ValueType::Utf8)],
				measures: vec![
					cumulative("read_bytes", ValueType::Uint8),
					cumulative("write_bytes", ValueType::Uint8),
					cumulative("read_ios", ValueType::Uint8),
					cumulative("write_ios", ValueType::Uint8),
					cumulative("discard_bytes", ValueType::Uint8),
					cumulative("discard_ios", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::ProcCgroupMemory => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROC_CGROUP_MEMORY,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("current", ValueType::Uint8),
					level_optional("max", ValueType::Uint8),
					level("anon", ValueType::Uint8),
					level("file", ValueType::Uint8),
					level("file_dirty", ValueType::Uint8),
					level("file_writeback", ValueType::Uint8),
					level("slab", ValueType::Uint8),
					level("sock", ValueType::Uint8),
					level("swap_current", ValueType::Uint8),
					level_optional("swap_max", ValueType::Uint8),
					cumulative("page_faults", ValueType::Uint8),
					cumulative("major_page_faults", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::ProcCgroupCpu => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROC_CGROUP_CPU,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					cumulative("usage", ValueType::Duration),
					cumulative("user", ValueType::Duration),
					cumulative("system", ValueType::Duration),
					cumulative_optional("periods", ValueType::Uint8),
					cumulative_optional("throttled_periods", ValueType::Uint8),
					cumulative_optional("throttled", ValueType::Duration),
				],
				has_total: false,
			},
			MetricsDomain::ProcCgroupPressure => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_PROC_CGROUP_PRESSURE,
				shape: DomainShape::Wide,
				dimensions: vec![dim("resource", ValueType::Utf8)],
				measures: vec![
					cumulative("some_stalled", ValueType::Duration),
					cumulative_optional("full_stalled", ValueType::Duration),
					level("some_avg10", ValueType::Float8),
					level("some_avg60", ValueType::Float8),
					level("some_avg300", ValueType::Float8),
					level_optional("full_avg10", ValueType::Float8),
					level_optional("full_avg60", ValueType::Float8),
					level_optional("full_avg300", ValueType::Float8),
				],
				has_total: false,
			},
			MetricsDomain::StoreMultiCommit => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_MULTI_COMMIT,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("current_bytes", ValueType::Uint8),
					level("historical_bytes", ValueType::Uint8),
					level("table_count", ValueType::Uint8),
					level("current_entries", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::StoreMultiRead => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_MULTI_READ,
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
					counter("installs", ValueType::Uint8),
					counter("installs_refused", ValueType::Uint8),
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
			MetricsDomain::StoreMultiPersistent => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_MULTI_PERSISTENT,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("used", ValueType::Uint8),
					level("connections_sampled", ValueType::Uint8),
					level("connections_total", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					counter("persistent_probes", ValueType::Uint8),
					counter("persistent_absent", ValueType::Uint8),
					level("filter_fill_ratio", ValueType::Float8),
					level("filter_estimated_keys", ValueType::Uint8),
					level("filter_rejected", ValueType::Uint8),
					level("filter_enabled", ValueType::Uint8),
					level("filter_rebuilds", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreSingleCommit => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_SINGLE_COMMIT,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("resident_entries", ValueType::Uint8),
					level("resident_bytes", ValueType::Uint8),
				],
				has_total: false,
			},
			MetricsDomain::StoreSinglePersistent => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_SINGLE_PERSISTENT,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("used", ValueType::Uint8),
					level("connections_sampled", ValueType::Uint8),
					level("connections_total", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					counter("persistent_probes", ValueType::Uint8),
					counter("persistent_absent", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreOperatorPoint => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_POINT,
				shape: DomainShape::Wide,
				dimensions: vec![dim("shard", ValueType::Uint2)],
				measures: vec![
					level("used", ValueType::Uint8),
					level("limit", ValueType::Uint8),
					level("entries", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					counter("insertions", ValueType::Uint8),
					counter("evictions", ValueType::Uint8),
					counter("fills_started", ValueType::Uint8),
					counter("fills_dirty_aborted", ValueType::Uint8),
					counter("fills_duplicate", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreOperatorPointKeyspace => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_POINT_KEYSPACE,
				shape: DomainShape::Wide,
				dimensions: vec![dim("keyspace", ValueType::Utf8)],
				measures: vec![
					level("used", ValueType::Uint8),
					level("entries", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					counter("insertions", ValueType::Uint8),
					counter("evictions", ValueType::Uint8),
					counter("fills_started", ValueType::Uint8),
					counter("fills_dirty_aborted", ValueType::Uint8),
					counter("fills_duplicate", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreOperatorRange => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_RANGE,
				shape: DomainShape::Wide,
				dimensions: vec![dim("shard", ValueType::Uint2)],
				measures: vec![
					level("used", ValueType::Uint8),
					level("limit", ValueType::Uint8),
					level("partitions", ValueType::Uint8),
					level("entries", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					counter("installs", ValueType::Uint8),
					counter("installs_refused", ValueType::Uint8),
					counter("installs_raced", ValueType::Uint8),
					counter("evictions", ValueType::Uint8),
					counter("point_hits", ValueType::Uint8),
					counter("point_misses", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreOperatorRangeKeyspace => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_RANGE_KEYSPACE,
				shape: DomainShape::Wide,
				dimensions: vec![dim("keyspace", ValueType::Utf8)],
				measures: vec![
					level("used", ValueType::Uint8),
					level("partitions", ValueType::Uint8),
					level("intervals", ValueType::Uint8),
					level("entries", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					counter("installs", ValueType::Uint8),
					counter("installs_refused", ValueType::Uint8),
					counter("installs_raced", ValueType::Uint8),
					counter("evictions", ValueType::Uint8),
					counter("point_hits", ValueType::Uint8),
					counter("point_misses", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreOperatorPersistent => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_PERSISTENT,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("used", ValueType::Uint8),
					level("connections_sampled", ValueType::Uint8),
					level("connections_total", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					level("filter_fill_ratio", ValueType::Float8),
					level("filter_estimated_keys", ValueType::Uint8),
					level("filter_rejected", ValueType::Uint8),
					level("filter_enabled", ValueType::Uint8),
					level("filter_rebuilds", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreCdcCommit => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_CDC_COMMIT,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("resident_bytes", ValueType::Uint8),
					level("entries", ValueType::Uint8),
					counter("blocks_cut", ValueType::Uint8),
					counter("stalls", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreCdcRead => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_CDC_READ,
				shape: DomainShape::Wide,
				dimensions: vec![dim("shard", ValueType::Uint2)],
				measures: vec![
					level("used", ValueType::Uint8),
					level("limit", ValueType::Uint8),
					level("blocks", ValueType::Uint8),
					counter("hits", ValueType::Uint8),
					counter("misses", ValueType::Uint8),
					counter("insertions", ValueType::Uint8),
					counter("evictions", ValueType::Uint8),
				],
				has_total: true,
			},
			MetricsDomain::StoreCdcPersistent => DomainSpec {
				domain: self,
				namespace: NamespaceId::SYSTEM_METRICS_STORE_CDC_PERSISTENT,
				shape: DomainShape::Wide,
				dimensions: Vec::new(),
				measures: vec![
					level("blocks", ValueType::Uint8),
					level("stored_bytes", ValueType::Uint8),
					counter("appends", ValueType::Uint8),
					counter("loads", ValueType::Uint8),
					counter("drops", ValueType::Uint8),
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
