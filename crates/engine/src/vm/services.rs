// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_auth::registry::AuthenticationRegistry;
use reifydb_catalog::{
	catalog::Catalog,
	metrics::storage::metrics::MetricsReader,
	vtable::{system::operator_libary::OperatorLibrary, user::registry::UserVTableRegistry},
};
use reifydb_core::util::ioc::IocContainer;
use reifydb_extension::transform::registry::Transforms;
#[cfg(test)]
use reifydb_routine::{
	function::default_in_process_functions, monoid::default_in_process_monoids,
	procedure::default_in_process_procedures,
};
use reifydb_routine_abi::{Procedure, registry::Routines};
use reifydb_rql::compiler::Compiler;
use reifydb_runtime::context::RuntimeContext;
#[cfg(test)]
use reifydb_runtime::context::clock::Clock;
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::sumtype::VariantRef;

#[cfg(not(reifydb_single_threaded))]
use crate::remote::RemoteRegistry;
use crate::vm::flow_lineage::ViewLineage;

pub struct EngineConfig {
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
	pub transforms: Transforms,
	pub ioc: IocContainer,
	pub auth_registry: Arc<AuthenticationRegistry>,
	#[cfg(not(reifydb_single_threaded))]
	pub remote_registry: Option<RemoteRegistry>,
}

pub struct Services {
	pub catalog: Catalog,
	pub runtime_context: RuntimeContext,
	pub compiler: Compiler,
	pub routines: Routines,
	pub transforms: Transforms,
	pub operators: OperatorLibrary,
	pub virtual_table_registry: UserVTableRegistry,
	pub metrics_reader: MetricsReader<SingleStore>,
	pub ioc: IocContainer,
	pub auth_registry: Arc<AuthenticationRegistry>,
	pub view_lineage: ViewLineage,
	#[cfg(not(reifydb_single_threaded))]
	pub remote_registry: Option<RemoteRegistry>,
}

impl Services {
	pub fn new(
		catalog: Catalog,
		config: EngineConfig,
		operator_store: OperatorLibrary,
		metrics_reader: MetricsReader<SingleStore>,
	) -> Self {
		Self {
			compiler: Compiler::new(catalog.clone()),
			catalog,
			runtime_context: config.runtime_context,
			routines: config.routines,
			transforms: config.transforms,
			operators: operator_store,
			virtual_table_registry: UserVTableRegistry::new(),
			metrics_reader,
			ioc: config.ioc,
			auth_registry: config.auth_registry,
			view_lineage: ViewLineage::default(),
			#[cfg(not(reifydb_single_threaded))]
			remote_registry: config.remote_registry,
		}
	}

	pub fn get_handlers(&self, txn: &mut Transaction<'_>, variant: VariantRef) -> Vec<Arc<dyn Procedure>> {
		self.routines.get_handlers(&self.catalog, txn, variant)
	}

	pub fn get_procedure(&self, name: &str) -> Option<Arc<dyn Procedure>> {
		self.routines.get_procedure(name)
	}

	#[cfg(test)]
	pub fn testing() -> Arc<Self> {
		let store = SingleStore::testing_memory();

		let routines_builder = Routines::builder();
		let routines_builder = default_in_process_functions(routines_builder);
		let routines_builder = default_in_process_procedures(routines_builder);
		let routines_builder = default_in_process_monoids(routines_builder);
		let routines = routines_builder.configure();

		let services = Self::new(
			Catalog::testing(),
			EngineConfig {
				runtime_context: RuntimeContext::with_clock(Clock::Real),
				routines,
				transforms: Transforms::empty(),
				ioc: IocContainer::new(),
				auth_registry: Arc::new(AuthenticationRegistry::default()),
				#[cfg(not(reifydb_single_threaded))]
				remote_registry: None,
			},
			OperatorLibrary::new(),
			MetricsReader::new(store),
		);
		Arc::new(services)
	}
}
