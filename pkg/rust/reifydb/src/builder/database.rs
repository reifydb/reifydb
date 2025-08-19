// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{sync::Arc, time::Duration};

use reifydb_core::{
	hook::Hooks, interceptor::StandardInterceptorBuilder,
	interface::Transaction, ioc::IocContainer,
};
use reifydb_engine::StandardEngine;

#[cfg(feature = "sub_flow")]
use crate::subsystem::FlowSubsystemFactory;
use crate::{
	database::{Database, DatabaseConfig},
	health::HealthMonitor,
	subsystem::{
		SubsystemFactory, Subsystems,
		logging::LoggingSubsystemFactory,
		worker_pool::WorkerPoolSubsystemFactory,
	},
};

pub struct DatabaseBuilder<T: Transaction> {
	config: DatabaseConfig,
	interceptors: StandardInterceptorBuilder<T>,
	subsystems: Vec<Box<dyn SubsystemFactory<T>>>,
	ioc: IocContainer,
}

impl<T: Transaction> DatabaseBuilder<T> {
	/// Create a new builder with engine components (new factory-based
	/// approach)
	#[allow(unused_mut)]
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
	) -> Self {
		// Create IoC container and register initial dependencies
		let ioc = IocContainer::new()
			.register(hooks.clone())
			.register(versioned.clone())
			.register(unversioned.clone())
			.register(cdc.clone());

		let mut result = Self {
			config: DatabaseConfig::default(),
			interceptors: StandardInterceptorBuilder::new(),
			subsystems: Vec::new(),
			ioc,
		};

		// Add logging subsystem first so it's initialized before other subsystems
		result = result.add_subsystem_factory(Box::new(
			LoggingSubsystemFactory::new(),
		));
		
		result = result.add_subsystem_factory(Box::new(
			WorkerPoolSubsystemFactory::new(),
		));

		#[cfg(feature = "sub_flow")]
		{
			result = result.add_subsystem_factory(Box::new(
				FlowSubsystemFactory::new(),
			));
		}

		result
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
		factory: Box<dyn SubsystemFactory<T>>,
	) -> Self {
		self.subsystems.push(factory);
		self
	}

	/// Add interceptors directly to the builder
	pub fn with_interceptor_builder(
		mut self,
		builder: StandardInterceptorBuilder<T>,
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
		// Phase 1: Collect interceptors from all factories (passing
		// IoC)
		for factory in &self.subsystems {
			self.interceptors = factory.provide_interceptors(
				self.interceptors,
				&self.ioc,
			);
		}

		// Phase 2: Create engine with all interceptors
		// Retrieve components from IoC container
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

		// Phase 3: Create subsystems from factories (they get engine
		// from IoC)
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
