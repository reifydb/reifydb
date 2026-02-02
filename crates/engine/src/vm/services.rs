// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::{
	catalog::Catalog,
	vtable::{system::flow_operator_store::FlowOperatorStore, user::registry::UserVTableRegistry},
};
use reifydb_core::util::ioc::IocContainer;
use reifydb_function::{math, registry::Functions, series, subscription};
use reifydb_metric::metric::MetricReader;
use reifydb_store_single::SingleStore;

/// Services is a container for shared resources used throughout the execution engine.
///
/// This struct provides a single location for all the shared resources that the VM,
/// query operators, and other components need access to.
pub struct Services {
	pub catalog: Catalog,
	pub functions: Functions,
	pub flow_operator_store: FlowOperatorStore,
	pub virtual_table_registry: UserVTableRegistry,
	pub stats_reader: MetricReader<SingleStore>,
	pub ioc: IocContainer,
}

impl Services {
	pub fn new(
		catalog: Catalog,
		functions: Functions,
		flow_operator_store: FlowOperatorStore,
		stats_reader: MetricReader<SingleStore>,
		ioc: IocContainer,
	) -> Self {
		Self {
			catalog,
			functions,
			flow_operator_store,
			virtual_table_registry: UserVTableRegistry::new(),
			stats_reader,
			ioc,
		}
	}

	#[allow(dead_code)]
	pub fn testing() -> Arc<Self> {
		let store = SingleStore::testing_memory();
		Arc::new(Self::new(
			Catalog::testing(),
			Functions::builder()
				.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
				.register_aggregate("math::min", math::aggregate::min::Min::new)
				.register_aggregate("math::max", math::aggregate::max::Max::new)
				.register_aggregate("math::avg", math::aggregate::avg::Avg::new)
				.register_aggregate("math::count", math::aggregate::count::Count::new)
				.register_scalar("math::abs", math::scalar::abs::Abs::new)
				.register_scalar("math::avg", math::scalar::avg::Avg::new)
				.register_generator("generate_series", series::GenerateSeries::new)
				.register_generator(
					"inspect_subscription",
					subscription::inspect::InspectSubscription::new,
				)
				.build(),
			FlowOperatorStore::new(),
			MetricReader::new(store),
			IocContainer::new(),
		))
	}
}
