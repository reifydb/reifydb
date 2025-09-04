// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::EventBus,
	interceptor::{RegisterInterceptor, StandardInterceptorBuilder},
	interface::{
		CdcTransaction, UnversionedTransaction, VersionedTransaction,
		subsystem::SubsystemFactory,
	},
};
use reifydb_engine::{EngineTransaction, StandardCommandTransaction};
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::{LoggingBuilder, LoggingSubsystemFactory};

use super::{DatabaseBuilder, traits::WithSubsystem};
use crate::Database;

pub struct EmbeddedBuilder<
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
> {
	versioned: VT,
	unversioned: UT,
	cdc: C,
	eventbus: EventBus,
	interceptors: StandardInterceptorBuilder<
		StandardCommandTransaction<EngineTransaction<VT, UT, C>>,
	>,
	subsystem_factories: Vec<
		Box<
			dyn SubsystemFactory<
				StandardCommandTransaction<
					EngineTransaction<VT, UT, C>,
				>,
			>,
		>,
	>,
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction, C: CdcTransaction>
	EmbeddedBuilder<VT, UT, C>
{
	pub fn new(
		versioned: VT,
		unversioned: UT,
		cdc: C,
		eventbus: EventBus,
	) -> Self {
		Self {
			versioned,
			unversioned,
			cdc,
			eventbus,
			interceptors: StandardInterceptorBuilder::new(),
			subsystem_factories: Vec::new(),
		}
	}

	pub fn intercept<I>(mut self, interceptor: I) -> Self
	where
		I: RegisterInterceptor<
				StandardCommandTransaction<
					EngineTransaction<VT, UT, C>,
				>,
			> + Send
			+ Sync
			+ Clone
			+ 'static,
	{
		self.interceptors =
			self.interceptors.add_factory(move |interceptors| {
				interceptors.register(interceptor.clone());
			});
		self
	}

	pub fn build(self) -> crate::Result<Database<VT, UT, C>> {
		let mut builder = DatabaseBuilder::new(
			self.versioned,
			self.unversioned,
			self.cdc,
			self.eventbus,
		)
		.with_interceptor_builder(self.interceptors);

		// Add any custom subsystem factories configured via fluent API
		for factory in self.subsystem_factories {
			builder = builder.add_subsystem_factory(factory);
		}

		// Add default subsystems (worker pool, flow, etc.)
		// This will only add logging if no subsystems were configured
		builder = builder.with_default_subsystems();

		builder.build()
	}
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction, C: CdcTransaction>
	WithSubsystem<EngineTransaction<VT, UT, C>> for EmbeddedBuilder<VT, UT, C>
{
	#[cfg(feature = "sub_logging")]
	fn with_logging<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static,
	{
		self.subsystem_factories.push(Box::new(
			LoggingSubsystemFactory::with_configurator(
				configurator,
			),
		));
		self
	}

	fn with_subsystem(
		mut self,
		factory: Box<
			dyn SubsystemFactory<
				StandardCommandTransaction<
					EngineTransaction<VT, UT, C>,
				>,
			>,
		>,
	) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}
