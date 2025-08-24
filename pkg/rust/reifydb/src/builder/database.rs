// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::Hooks,
	interceptor::StandardInterceptorBuilder,
	interface::{subsystem::SubsystemFactory, Transaction},
	ioc::IocContainer,
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowSubsystemFactory;
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::LoggingSubsystemFactory;
#[cfg(feature = "sub_workerpool")]
use reifydb_sub_workerpool::WorkerPoolSubsystemFactory;
use std::{sync::Arc, time::Duration};

use crate::{
	database::{Database, DatabaseConfig},
	health::HealthMonitor,
	subsystem::Subsystems,
};

pub struct DatabaseBuilder<T: Transaction> {
	config: DatabaseConfig,
	interceptors: StandardInterceptorBuilder<StandardCommandTransaction<T>>,
	subsystems: Vec<
		Box<dyn SubsystemFactory<StandardCommandTransaction<T>>>,
	>,
	ioc: IocContainer,
}

impl<T: Transaction> DatabaseBuilder<T> {
	#[allow(unused_mut)]
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
	) -> Self {
		let ioc = IocContainer::new()
			.register(hooks.clone())
			.register(versioned.clone())
			.register(unversioned.clone())
			.register(cdc.clone());

		let result = Self {
			config: DatabaseConfig::default(),
			interceptors: StandardInterceptorBuilder::new(),
			subsystems: Vec::new(),
			ioc,
		};

		result
	}

	/// Add default subsystems that are always required
	pub fn with_default_subsystems(mut self) -> Self {
		// Add default logging subsystem first so it's initialized
		// before other subsystems Note: This can be overridden by
		// adding a custom logging factory before calling this
		#[cfg(feature = "sub_logging")]
		if self.subsystems.is_empty() {
			self = self.add_subsystem_factory(Box::new(
				LoggingSubsystemFactory::new(),
			));
		}

		// Add worker pool subsystem if feature enabled
		#[cfg(feature = "sub_workerpool")]
		{
			self = self.add_subsystem_factory(Box::new(
				WorkerPoolSubsystemFactory::new(),
			));
		}

		#[cfg(feature = "sub_flow")]
		{
			self = self.add_subsystem_factory(Box::new(
				FlowSubsystemFactory::new(),
			));
		}

		self
	}

	pub fn with_graceful_shutdown_timeout(
		mut self,
		timeout: Duration,
	) -> Self {
		self.config =
			self.config.with_graceful_shutdown_timeout(timeout);
		self
	}

	pub fn with_health_check_interval(
		mut self,
		interval: Duration,
	) -> Self {
		self.config = self.config.with_health_check_interval(interval);
		self
	}

	pub fn with_max_startup_time(mut self, timeout: Duration) -> Self {
		self.config = self.config.with_max_startup_time(timeout);
		self
	}

	pub fn with_config(mut self, config: DatabaseConfig) -> Self {
		self.config = config;
		self
	}

	pub fn add_subsystem_factory(
		mut self,
		factory: Box<
			dyn SubsystemFactory<StandardCommandTransaction<T>>,
		>,
	) -> Self {
		self.subsystems.push(factory);
		self
	}

	pub fn with_interceptor_builder(
		mut self,
		builder: StandardInterceptorBuilder<
			StandardCommandTransaction<T>,
		>,
	) -> Self {
		self.interceptors = builder;
		self
	}

	pub fn config(&self) -> &DatabaseConfig {
		&self.config
	}

	pub fn subsystem_count(&self) -> usize {
		self.subsystems.len()
	}

	pub fn build(mut self) -> crate::Result<Database<T>> {
		// Collect interceptors from all factories
		for factory in &self.subsystems {
			self.interceptors = factory.provide_interceptors(
				self.interceptors,
				&self.ioc,
			);
		}

		let versioned = self.ioc.resolve::<T::Versioned>()?;
		let unversioned = self.ioc.resolve::<T::Unversioned>()?;
		let cdc = self.ioc.resolve::<T::Cdc>()?;
		let hooks = self.ioc.resolve::<Hooks>()?;

		let engine = StandardEngine::new(
			versioned,
			unversioned,
			cdc,
			hooks,
			Box::new(self.interceptors.build()),
		);

		self.ioc = self.ioc.register(engine.clone());

		// Create subsystems from factories
		let health_monitor = Arc::new(HealthMonitor::new());
		let mut subsystems =
			Subsystems::new(Arc::clone(&health_monitor));

		for factory in self.subsystems {
			let subsystem = factory.create(&self.ioc)?;
			subsystems.add_subsystem(subsystem);
		}

		Ok(Database::new(
			engine,
			subsystems,
			self.config,
			health_monitor,
		))
	}
}

impl<T: Transaction> DatabaseBuilder<T> {
	pub fn development_config(self) -> Self {
		self.with_graceful_shutdown_timeout(Duration::from_secs(10))
			.with_health_check_interval(Duration::from_secs(2))
			.with_max_startup_time(Duration::from_secs(30))
	}

	pub fn production_config(self) -> Self {
		self.with_graceful_shutdown_timeout(Duration::from_secs(60))
			.with_health_check_interval(Duration::from_secs(10))
			.with_max_startup_time(Duration::from_secs(120))
	}

	pub fn testing_config(self) -> Self {
		self.with_graceful_shutdown_timeout(Duration::from_secs(5))
			.with_health_check_interval(Duration::from_secs(1))
			.with_max_startup_time(Duration::from_secs(10))
	}
}
