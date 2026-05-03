// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_auth::registry::AuthenticationRegistry;
use reifydb_catalog::{
	catalog::Catalog,
	vtable::{system::flow_operator_store::SystemFlowOperatorStore, user::registry::UserVTableRegistry},
};
use reifydb_core::util::ioc::IocContainer;
use reifydb_extension::transform::registry::Transforms;
use reifydb_metric::storage::metric::MetricReader;
use reifydb_routine::{
	function::default_native_functions,
	procedure::default_native_procedures,
	routine::{Procedure, registry::Routines},
};
use reifydb_rql::compiler::Compiler;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_store_single::SingleStore;
use reifydb_type::value::sumtype::VariantRef;

#[cfg(not(reifydb_single_threaded))]
use crate::remote::RemoteRegistry;

pub struct EngineConfig {
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
	pub transforms: Transforms,
	pub ioc: IocContainer,
	#[cfg(not(reifydb_single_threaded))]
	pub remote_registry: Option<RemoteRegistry>,
}

pub struct Services {
	pub catalog: Catalog,
	pub runtime_context: RuntimeContext,
	pub compiler: Compiler,
	pub routines: Routines,
	pub transforms: Transforms,
	pub flow_operator_store: SystemFlowOperatorStore,
	pub virtual_table_registry: UserVTableRegistry,
	pub stats_reader: MetricReader<SingleStore>,
	pub ioc: IocContainer,
	pub auth_registry: AuthenticationRegistry,
	#[cfg(not(reifydb_single_threaded))]
	pub remote_registry: Option<RemoteRegistry>,
}

impl Services {
	pub fn new(
		catalog: Catalog,
		config: EngineConfig,
		flow_operator_store: SystemFlowOperatorStore,
		stats_reader: MetricReader<SingleStore>,
	) -> Self {
		let auth_registry = AuthenticationRegistry::new(config.runtime_context.clock.clone());
		Self {
			compiler: Compiler::new(catalog.clone()),
			catalog,
			runtime_context: config.runtime_context,
			routines: config.routines,
			transforms: config.transforms,
			flow_operator_store,
			virtual_table_registry: UserVTableRegistry::new(),
			stats_reader,
			ioc: config.ioc,
			auth_registry,
			#[cfg(not(reifydb_single_threaded))]
			remote_registry: config.remote_registry,
		}
	}

	pub fn get_handlers(&self, variant: VariantRef) -> Vec<Arc<dyn Procedure>> {
		self.routines.get_handlers(&self.catalog.materialized, variant)
	}

	pub fn get_procedure(&self, name: &str) -> Option<Arc<dyn Procedure>> {
		self.routines.get_procedure(name)
	}

	#[allow(dead_code)]
	pub fn testing() -> Arc<Self> {
		let store = SingleStore::testing_memory();

		let routines_builder = Routines::builder();
		let routines_builder = default_native_functions(routines_builder);
		let routines_builder = default_native_procedures(routines_builder);
		let routines = routines_builder.configure();

		let mut services = Self::new(
			Catalog::testing(),
			EngineConfig {
				runtime_context: RuntimeContext::with_clock(Clock::Real),
				routines,
				transforms: Transforms::empty(),
				ioc: IocContainer::new(),
				#[cfg(not(reifydb_single_threaded))]
				remote_registry: None,
			},
			SystemFlowOperatorStore::new(),
			MetricReader::new(store),
		);
		services.auth_registry = AuthenticationRegistry::default();
		Arc::new(services)
	}
}
