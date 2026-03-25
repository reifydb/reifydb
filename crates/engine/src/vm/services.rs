// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_auth::registry::AuthenticationRegistry;
use reifydb_catalog::{
	catalog::Catalog,
	vtable::{system::flow_operator_store::FlowOperatorStore, user::registry::UserVTableRegistry},
};
use reifydb_core::util::ioc::IocContainer;
use reifydb_function::{is, math, registry::Functions, series, subscription};
use reifydb_metric::metric::MetricReader;
use reifydb_rql::compiler::Compiler;
use reifydb_runtime::context::RuntimeContext;
use reifydb_store_single::SingleStore;
use reifydb_type::value::sumtype::SumTypeId;

#[cfg(not(target_arch = "wasm32"))]
use crate::remote::RemoteRegistry;
use crate::{
	procedure::{Procedure, registry::Procedures},
	transform::registry::Transforms,
};

/// Services is a container for shared resources used throughout the execution engine.
///
/// This struct provides a single location for all the shared resources that the VM,
/// query operators, and other components need access to.
pub struct Services {
	pub catalog: Catalog,
	pub runtime_context: RuntimeContext,
	pub compiler: Compiler,
	pub functions: Functions,
	pub procedures: Procedures,
	pub transforms: Transforms,
	pub flow_operator_store: FlowOperatorStore,
	pub virtual_table_registry: UserVTableRegistry,
	pub stats_reader: MetricReader<SingleStore>,
	pub ioc: IocContainer,
	pub auth_registry: AuthenticationRegistry,
	#[cfg(not(target_arch = "wasm32"))]
	pub remote_registry: Option<RemoteRegistry>,
}

impl Services {
	pub fn new(
		catalog: Catalog,
		runtime_context: RuntimeContext,
		functions: Functions,
		procedures: Procedures,
		transforms: Transforms,
		flow_operator_store: FlowOperatorStore,
		stats_reader: MetricReader<SingleStore>,
		ioc: IocContainer,
		#[cfg(not(target_arch = "wasm32"))] remote_registry: Option<RemoteRegistry>,
	) -> Self {
		let auth_registry = AuthenticationRegistry::new(runtime_context.clock.clone());
		Self {
			compiler: Compiler::new(catalog.clone()),
			catalog,
			runtime_context,
			functions,
			procedures,
			transforms,
			flow_operator_store,
			virtual_table_registry: UserVTableRegistry::new(),
			stats_reader,
			ioc,
			auth_registry,
			#[cfg(not(target_arch = "wasm32"))]
			remote_registry,
		}
	}

	pub fn get_handlers(&self, sumtype_id: SumTypeId, variant_tag: u8) -> Vec<Box<dyn Procedure>> {
		self.procedures.get_handlers(&self.catalog.materialized, sumtype_id, variant_tag)
	}

	pub fn get_procedure(&self, name: &str) -> Option<Box<dyn Procedure>> {
		self.procedures.get_procedure(name)
	}

	#[allow(dead_code)]
	pub fn testing() -> Arc<Self> {
		let store = SingleStore::testing_memory();
		let mut services = Self::new(
			Catalog::testing(),
			RuntimeContext::default(),
			Functions::builder()
				.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
				.register_aggregate("math::min", math::aggregate::min::Min::new)
				.register_aggregate("math::max", math::aggregate::max::Max::new)
				.register_aggregate("math::avg", math::aggregate::avg::Avg::new)
				.register_aggregate("math::count", math::aggregate::count::Count::new)
				.register_scalar("math::abs", math::scalar::abs::Abs::new)
				.register_scalar("math::avg", math::scalar::avg::Avg::new)
				.register_scalar("is::some", is::some::IsSome::new)
				.register_scalar("is::none", is::none::IsNone::new)
				.register_scalar("is::type", is::r#type::IsType::new)
				.register_scalar("gen::series", series::Series::new)
				.register_generator("generate_series", series::GenerateSeries::new)
				.register_generator(
					"inspect_subscription",
					subscription::inspect::InspectSubscription::new,
				)
				.build(),
			Procedures::empty(),
			Transforms::empty(),
			FlowOperatorStore::new(),
			MetricReader::new(store),
			IocContainer::new(),
			#[cfg(not(target_arch = "wasm32"))]
			None,
		);
		services.auth_registry = AuthenticationRegistry::default();
		Arc::new(services)
	}
}
