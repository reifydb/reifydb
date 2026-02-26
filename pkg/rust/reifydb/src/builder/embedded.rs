// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::path::PathBuf;

use reifydb_engine::{procedure::registry::ProceduresBuilder, transform::registry::Transforms};
use reifydb_function::registry::FunctionsBuilder;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowBuilder;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingBuilder;
use reifydb_transaction::interceptor::builder::InterceptorBuilder;

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::{
	Database, Migration,
	api::{StorageFactory, transaction},
};

pub struct EmbeddedBuilder {
	storage_factory: StorageFactory,
	runtime: Option<SharedRuntime>,
	runtime_config: Option<SharedRuntimeConfig>,
	interceptors: InterceptorBuilder,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	procedures_configurator: Option<Box<dyn FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static>>,
	handlers_configurator: Option<Box<dyn FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static>>,
	#[cfg(reifydb_target = "native")]
	procedure_dir: Option<PathBuf>,
	wasm_procedure_dir: Option<PathBuf>,
	transforms: Option<Transforms>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static>>,
	migrations: Vec<Migration>,
}

impl EmbeddedBuilder {
	pub fn new(storage_factory: StorageFactory) -> Self {
		Self {
			storage_factory,
			runtime: None,
			runtime_config: None,
			interceptors: InterceptorBuilder::new(),
			subsystem_factories: Vec::new(),
			functions_configurator: None,
			procedures_configurator: None,
			handlers_configurator: None,
			#[cfg(reifydb_target = "native")]
			procedure_dir: None,
			wasm_procedure_dir: None,
			transforms: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
			migrations: Vec::new(),
		}
	}

	/// Provide a pre-built shared runtime.
	///
	/// When set, this runtime is used directly and `with_runtime_config` is ignored.
	pub fn with_runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}

	/// Configure the shared runtime.
	///
	/// If not set, a default configuration will be used.
	/// Ignored if `with_runtime` was called.
	pub fn with_runtime_config(mut self, config: SharedRuntimeConfig) -> Self {
		self.runtime_config = Some(config);
		self
	}

	pub fn with_functions<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static,
	{
		self.functions_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_procedures<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static,
	{
		self.procedures_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_handlers<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static,
	{
		self.handlers_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(reifydb_target = "native")]
	pub fn with_procedure_dir(mut self, dir: impl Into<PathBuf>) -> Self {
		self.procedure_dir = Some(dir.into());
		self
	}

	pub fn with_wasm_procedure_dir(mut self, dir: impl Into<PathBuf>) -> Self {
		self.wasm_procedure_dir = Some(dir.into());
		self
	}

	pub fn with_transforms(mut self, transforms: Transforms) -> Self {
		self.transforms = Some(transforms);
		self
	}

	/// Register migrations to be applied during `Database::start()`.
	///
	/// Migrations are stored in the database on first encounter and
	/// applied in name order. Already-applied migrations are skipped.
	pub fn with_migrations(mut self, migrations: Vec<Migration>) -> Self {
		self.migrations = migrations;
		self
	}

	pub fn build(self) -> crate::Result<Database> {
		let runtime = match self.runtime {
			Some(rt) => rt,
			None => SharedRuntime::from_config(self.runtime_config.unwrap_or_default()),
		};

		let actor_system = runtime.actor_system().scope();
		let (multi_store, single_store, transaction_single, eventbus) =
			self.storage_factory.create(&actor_system);
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			actor_system.clone(),
			runtime.clock().clone(),
		);

		let mut builder = DatabaseBuilder::new(multi, single, eventbus)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime.clone())
			.with_actor_system(actor_system)
			.with_stores(multi_store, single_store);

		if let Some(configurator) = self.functions_configurator {
			builder = builder.with_functions_configurator(configurator);
		}

		if let Some(configurator) = self.procedures_configurator {
			builder = builder.with_procedures_configurator(configurator);
		}

		if let Some(configurator) = self.handlers_configurator {
			builder = builder.with_handlers_configurator(configurator);
		}

		#[cfg(reifydb_target = "native")]
		if let Some(dir) = self.procedure_dir {
			builder = builder.with_procedure_dir(dir);
		}

		if let Some(dir) = self.wasm_procedure_dir {
			builder = builder.with_wasm_procedure_dir(dir);
		}

		if let Some(transforms) = self.transforms {
			builder = builder.with_transforms(transforms);
		}

		#[cfg(feature = "sub_tracing")]
		if let Some(configurator) = self.tracing_configurator {
			builder = builder.with_tracing(configurator);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}

		for factory in self.subsystem_factories {
			builder = builder.add_subsystem_factory(factory);
		}

		if !self.migrations.is_empty() {
			builder = builder.with_migrations(self.migrations);
		}

		builder.build()
	}
}

impl WithSubsystem for EmbeddedBuilder {
	#[cfg(feature = "sub_tracing")]
	fn with_tracing<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static,
	{
		self.tracing_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static,
	{
		self.flow_configurator = Some(Box::new(configurator));
		self
	}

	fn with_subsystem(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}

impl WithInterceptorBuilder for EmbeddedBuilder {
	fn interceptor_builder_mut(&mut self) -> &mut InterceptorBuilder {
		&mut self.interceptors
	}
}
