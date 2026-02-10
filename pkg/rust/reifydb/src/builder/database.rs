// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_auth::AuthVersion;
use reifydb_catalog::{
	CatalogVersion,
	catalog::Catalog,
	materialized::{MaterializedCatalog, load::MaterializedCatalogLoader},
	schema::{SchemaRegistry, load::SchemaRegistryLoader},
	system::SystemCatalog,
};
use reifydb_cdc::{
	CdcVersion,
	produce::producer::{CdcProducerEventListener, spawn_cdc_producer},
	storage::CdcStore,
};
use reifydb_core::{
	CoreVersion,
	event::{
		EventBus,
		metric::{CdcStatsDroppedEvent, CdcStatsRecordedEvent, StorageStatsRecordedEvent},
		transaction::PostCommitEvent,
	},
	interface::version::{ComponentType, HasVersion, SystemVersion},
	util::ioc::IocContainer,
};
use reifydb_engine::{EngineVersion, engine::StandardEngine};
use reifydb_function::{
	blob, clock, date, datetime, duration,
	flow::to_json::FlowNodeToJson,
	is, math, meta,
	registry::{Functions, FunctionsBuilder},
	series, subscription, text, time,
};
use reifydb_metric::worker::{
	CdcStatsDroppedListener, CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener,
};
use reifydb_rql::RqlVersion;
use reifydb_runtime::SharedRuntime;
use reifydb_store_multi::{MultiStore, MultiStoreVersion};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::{builder::FlowBuilder, subsystem::factory::FlowSubsystemFactory};
use reifydb_sub_task::factory::{TaskConfig, TaskSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingBuilder;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::factory::TracingSubsystemFactory;
use reifydb_transaction::{
	TransactionVersion, interceptor::builder::StandardInterceptorBuilder, multi::transaction::MultiTransaction,
	single::SingleTransaction, transaction::query::QueryTransaction,
};
use tracing::debug;

use crate::{database::Database, health::HealthMonitor, subsystem::Subsystems, system::tasks::create_system_tasks};

pub struct DatabaseBuilder {
	interceptors: StandardInterceptorBuilder,
	factories: Vec<Box<dyn SubsystemFactory>>,
	ioc: IocContainer,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	multi_store: Option<MultiStore>,
	single_store: Option<SingleStore>,
	#[cfg(feature = "sub_tracing")]
	tracing_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(feature = "sub_flow")]
	flow_factory: Option<Box<dyn SubsystemFactory>>,
	task_factory: Option<Box<dyn SubsystemFactory>>,
}

impl DatabaseBuilder {
	#[allow(unused_mut)]
	pub fn new(multi: MultiTransaction, single: SingleTransaction, eventbus: EventBus) -> Self {
		let ioc = IocContainer::new()
			.register(MaterializedCatalog::new())
			.register(SchemaRegistry::new(single.clone()))
			.register(eventbus)
			.register(multi)
			.register(single);

		Self {
			interceptors: StandardInterceptorBuilder::new(),
			factories: Vec::new(),
			ioc,
			functions_configurator: None,
			multi_store: None,
			single_store: None,
			#[cfg(feature = "sub_tracing")]
			tracing_factory: None,
			#[cfg(feature = "sub_flow")]
			flow_factory: None,
			task_factory: None,
		}
	}

	/// Store the underlying MultiStore and SingleStore for metrics worker
	pub fn with_stores(mut self, multi: MultiStore, single: SingleStore) -> Self {
		self.multi_store = Some(multi);
		self.single_store = Some(single);
		self
	}

	#[cfg(feature = "sub_tracing")]
	pub fn with_tracing<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static,
	{
		self.tracing_factory = Some(Box::new(TracingSubsystemFactory::with_configurator(configurator)));
		self
	}

	#[cfg(feature = "sub_flow")]
	pub fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static,
	{
		self.flow_factory = Some(Box::new(FlowSubsystemFactory::with_configurator(configurator)));
		self
	}

	pub fn add_subsystem_factory(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.factories.push(factory);
		self
	}

	pub fn with_interceptor_builder(mut self, builder: StandardInterceptorBuilder) -> Self {
		self.interceptors = builder;
		self
	}

	pub fn with_functions_configurator<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static,
	{
		self.functions_configurator = Some(Box::new(configurator));
		self
	}

	/// Set the shared runtime for the database.
	///
	/// This registers the runtime in the IoC container so subsystems can resolve it.
	pub fn with_runtime(mut self, runtime: SharedRuntime) -> Self {
		self.ioc = self.ioc.register(runtime);
		self
	}

	pub fn subsystem_count(&self) -> usize {
		self.factories.len()
	}

	pub fn build(mut self) -> crate::Result<Database> {
		// Collect interceptors from all factories
		// Note: We process logging and flow factories separately before adding to self.factories

		#[cfg(feature = "sub_tracing")]
		if let Some(ref factory) = self.tracing_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(ref factory) = self.flow_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		if let Some(ref factory) = self.task_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		for factory in &self.factories {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		let catalog = self.ioc.resolve::<MaterializedCatalog>()?;
		let schema_registry = self.ioc.resolve::<SchemaRegistry>()?;
		let multi = self.ioc.resolve::<MultiTransaction>()?;
		let single = self.ioc.resolve::<SingleTransaction>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		Self::load_materialized_catalog(&multi, &single, &catalog)?;
		Self::load_schema_registry(&multi, &single, &schema_registry)?;

		let runtime = self.ioc.resolve::<SharedRuntime>()?;

		// Create and register CdcStore for CDC storage
		let cdc_store = CdcStore::memory();
		self.ioc = self.ioc.register(cdc_store.clone());

		// Get the underlying stores for workers
		let multi_store = self.multi_store.clone().expect("MultiStore must be set via with_stores()");
		let single_store = self.single_store.clone().expect("SingleStore must be set via with_stores()");

		// Create metrics worker and register event listeners
		let metrics_worker = Arc::new(MetricsWorker::new(
			MetricsWorkerConfig::default(),
			single_store.clone(),
			multi_store.clone(),
			eventbus.clone(),
		));
		eventbus.register::<StorageStatsRecordedEvent, _>(StorageStatsListener::new(metrics_worker.sender()));
		eventbus.register::<CdcStatsRecordedEvent, _>(CdcStatsListener::new(metrics_worker.sender()));
		eventbus.register::<CdcStatsDroppedEvent, _>(CdcStatsDroppedListener::new(metrics_worker.sender()));
		self.ioc.register_service::<Arc<MetricsWorker>>(metrics_worker);

		// Register single store in IoC for engine to access
		self.ioc = self.ioc.register(single_store);

		let default_builder = Functions::builder()
			.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
			.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
			.register_aggregate("math::min", math::aggregate::min::Min::new)
			.register_aggregate("math::max", math::aggregate::max::Max::new)
			.register_aggregate("math::avg", math::aggregate::avg::Avg::new)
			.register_aggregate("math::count", math::aggregate::count::Count::new)
			.register_scalar("flow_node::to_json", FlowNodeToJson::new)
			.register_scalar("clock::now", clock::now::Now::new)
			.register_scalar("clock::set", clock::set::Set::new)
			.register_scalar("clock::advance", clock::advance::Advance::new)
			.register_scalar("blob::b58", blob::b58::BlobB58::new)
			.register_scalar("blob::b64", blob::b64::BlobB64::new)
			.register_scalar("blob::b64url", blob::b64url::BlobB64url::new)
			.register_scalar("blob::hex", blob::hex::BlobHex::new)
			.register_scalar("blob::utf8", blob::utf8::BlobUtf8::new)
			.register_scalar("math::abs", math::scalar::abs::Abs::new)
			.register_scalar("math::acos", math::scalar::acos::Acos::new)
			.register_scalar("math::asin", math::scalar::asin::Asin::new)
			.register_scalar("math::atan", math::scalar::atan::Atan::new)
			.register_scalar("math::atan2", math::scalar::atan2::Atan2::new)
			.register_scalar("math::avg", math::scalar::avg::Avg::new)
			.register_scalar("math::ceil", math::scalar::ceil::Ceil::new)
			.register_scalar("math::clamp", math::scalar::clamp::Clamp::new)
			.register_scalar("math::cos", math::scalar::cos::Cos::new)
			.register_scalar("math::e", math::scalar::euler::Euler::new)
			.register_scalar("math::exp", math::scalar::exp::Exp::new)
			.register_scalar("math::floor", math::scalar::floor::Floor::new)
			.register_scalar("math::gcd", math::scalar::gcd::Gcd::new)
			.register_scalar("math::lcm", math::scalar::lcm::Lcm::new)
			.register_scalar("math::log", math::scalar::log::Log::new)
			.register_scalar("math::log10", math::scalar::log10::Log10::new)
			.register_scalar("math::log2", math::scalar::log2::Log2::new)
			.register_scalar("math::max", math::scalar::max::Max::new)
			.register_scalar("math::min", math::scalar::min::Min::new)
			.register_scalar("math::mod", math::scalar::modulo::Modulo::new)
			.register_scalar("math::pi", math::scalar::pi::Pi::new)
			.register_scalar("math::power", math::scalar::power::Power::new)
			.register_scalar("math::round", math::scalar::round::Round::new)
			.register_scalar("math::sign", math::scalar::sign::Sign::new)
			.register_scalar("math::sin", math::scalar::sin::Sin::new)
			.register_scalar("math::sqrt", math::scalar::sqrt::Sqrt::new)
			.register_scalar("math::tan", math::scalar::tan::Tan::new)
			.register_scalar("math::truncate", math::scalar::truncate::Truncate::new)
			.register_scalar("date::year", date::year::DateYear::new)
			.register_scalar("date::month", date::month::DateMonth::new)
			.register_scalar("date::day", date::day::DateDay::new)
			.register_scalar("date::day_of_year", date::day_of_year::DateDayOfYear::new)
			.register_scalar("date::day_of_week", date::day_of_week::DateDayOfWeek::new)
			.register_scalar("date::quarter", date::quarter::DateQuarter::new)
			.register_scalar("date::week", date::week::DateWeek::new)
			.register_scalar("date::is_leap_year", date::is_leap_year::DateIsLeapYear::new)
			.register_scalar("date::days_in_month", date::days_in_month::DateDaysInMonth::new)
			.register_scalar("date::end_of_month", date::end_of_month::DateEndOfMonth::new)
			.register_scalar("date::start_of_month", date::start_of_month::DateStartOfMonth::new)
			.register_scalar("date::start_of_year", date::start_of_year::DateStartOfYear::new)
			.register_scalar("date::new", date::new::DateNew::new)
			.register_scalar("date::now", date::now::DateNow::new)
			.register_scalar("date::add", date::add::DateAdd::new)
			.register_scalar("date::subtract", date::subtract::DateSubtract::new)
			.register_scalar("date::diff", date::diff::DateDiff::new)
			.register_scalar("date::trunc", date::trunc::DateTrunc::new)
			.register_scalar("date::age", date::age::DateAge::new)
			.register_scalar("date::format", date::format::DateFormat::new)
			.register_scalar("time::hour", time::hour::TimeHour::new)
			.register_scalar("time::minute", time::minute::TimeMinute::new)
			.register_scalar("time::second", time::second::TimeSecond::new)
			.register_scalar("time::nanosecond", time::nanosecond::TimeNanosecond::new)
			.register_scalar("time::new", time::new::TimeNew::new)
			.register_scalar("time::now", time::now::TimeNow::new)
			.register_scalar("time::add", time::add::TimeAdd::new)
			.register_scalar("time::subtract", time::subtract::TimeSubtract::new)
			.register_scalar("time::diff", time::diff::TimeDiff::new)
			.register_scalar("time::trunc", time::trunc::TimeTrunc::new)
			.register_scalar("time::age", time::age::TimeAge::new)
			.register_scalar("time::format", time::format::TimeFormat::new)
			.register_scalar("datetime::year", datetime::year::DateTimeYear::new)
			.register_scalar("datetime::month", datetime::month::DateTimeMonth::new)
			.register_scalar("datetime::day", datetime::day::DateTimeDay::new)
			.register_scalar("datetime::hour", datetime::hour::DateTimeHour::new)
			.register_scalar("datetime::minute", datetime::minute::DateTimeMinute::new)
			.register_scalar("datetime::second", datetime::second::DateTimeSecond::new)
			.register_scalar("datetime::nanosecond", datetime::nanosecond::DateTimeNanosecond::new)
			.register_scalar("datetime::day_of_year", datetime::day_of_year::DateTimeDayOfYear::new)
			.register_scalar("datetime::day_of_week", datetime::day_of_week::DateTimeDayOfWeek::new)
			.register_scalar("datetime::quarter", datetime::quarter::DateTimeQuarter::new)
			.register_scalar("datetime::week", datetime::week::DateTimeWeek::new)
			.register_scalar("datetime::date", datetime::date::DateTimeDate::new)
			.register_scalar("datetime::time", datetime::time::DateTimeTime::new)
			.register_scalar("datetime::epoch", datetime::epoch::DateTimeEpoch::new)
			.register_scalar("datetime::epoch_millis", datetime::epoch_millis::DateTimeEpochMillis::new)
			.register_scalar("datetime::new", datetime::new::DateTimeNew::new)
			.register_scalar("datetime::now", datetime::now::DateTimeNow::new)
			.register_scalar("datetime::from_epoch", datetime::from_epoch::DateTimeFromEpoch::new)
			.register_scalar(
				"datetime::from_epoch_millis",
				datetime::from_epoch_millis::DateTimeFromEpochMillis::new,
			)
			.register_scalar("datetime::add", datetime::add::DateTimeAdd::new)
			.register_scalar("datetime::subtract", datetime::subtract::DateTimeSubtract::new)
			.register_scalar("datetime::diff", datetime::diff::DateTimeDiff::new)
			.register_scalar("datetime::trunc", datetime::trunc::DateTimeTrunc::new)
			.register_scalar("datetime::age", datetime::age::DateTimeAge::new)
			.register_scalar("datetime::format", datetime::format::DateTimeFormat::new)
			.register_scalar("duration::years", duration::years::DurationYears::new)
			.register_scalar("duration::months", duration::months::DurationMonths::new)
			.register_scalar("duration::weeks", duration::weeks::DurationWeeks::new)
			.register_scalar("duration::days", duration::days::DurationDays::new)
			.register_scalar("duration::hours", duration::hours::DurationHours::new)
			.register_scalar("duration::minutes", duration::minutes::DurationMinutes::new)
			.register_scalar("duration::seconds", duration::seconds::DurationSeconds::new)
			.register_scalar("duration::millis", duration::millis::DurationMillis::new)
			.register_scalar("duration::year", duration::years::DurationYears::new)
			.register_scalar("duration::month", duration::months::DurationMonths::new)
			.register_scalar("duration::week", duration::weeks::DurationWeeks::new)
			.register_scalar("duration::day", duration::days::DurationDays::new)
			.register_scalar("duration::hour", duration::hours::DurationHours::new)
			.register_scalar("duration::minute", duration::minutes::DurationMinutes::new)
			.register_scalar("duration::second", duration::seconds::DurationSeconds::new)
			.register_scalar("duration::get_months", duration::get_months::DurationGetMonths::new)
			.register_scalar("duration::get_days", duration::get_days::DurationGetDays::new)
			.register_scalar("duration::get_nanos", duration::get_nanos::DurationGetNanos::new)
			.register_scalar("duration::add", duration::add::DurationAdd::new)
			.register_scalar("duration::subtract", duration::subtract::DurationSubtract::new)
			.register_scalar("duration::negate", duration::negate::DurationNegate::new)
			.register_scalar("duration::scale", duration::scale::DurationScale::new)
			.register_scalar("duration::trunc", duration::trunc::DurationTrunc::new)
			.register_scalar("duration::format", duration::format::DurationFormat::new)
			.register_scalar("text::ascii", text::ascii::TextAscii::new)
			.register_scalar("text::char", text::char::TextChar::new)
			.register_scalar("text::concat", text::concat::TextConcat::new)
			.register_scalar("text::contains", text::contains::TextContains::new)
			.register_scalar("text::count", text::count::TextCount::new)
			.register_scalar("text::ends_with", text::ends_with::TextEndsWith::new)
			.register_scalar("text::index_of", text::index_of::TextIndexOf::new)
			.register_scalar("text::pad_left", text::pad_left::TextPadLeft::new)
			.register_scalar("text::pad_right", text::pad_right::TextPadRight::new)
			.register_scalar("text::repeat", text::repeat::TextRepeat::new)
			.register_scalar("text::replace", text::replace::TextReplace::new)
			.register_scalar("text::reverse", text::reverse::TextReverse::new)
			.register_scalar("text::starts_with", text::starts_with::TextStartsWith::new)
			.register_scalar("text::length", text::length::TextLength::new)
			.register_scalar("text::trim", text::trim::TextTrim::new)
			.register_scalar("text::trim_end", text::trim_end::TextTrimEnd::new)
			.register_scalar("text::trim_start", text::trim_start::TextTrimStart::new)
			.register_scalar("text::upper", text::upper::TextUpper::new)
			.register_scalar("text::lower", text::lower::TextLower::new)
			.register_scalar("text::substring", text::substring::TextSubstring::new)
			.register_scalar("text::format_bytes", text::format_bytes::FormatBytes::new)
			.register_scalar("text::format_bytes_si", text::format_bytes_si::FormatBytesSi::new)
			.register_scalar("meta::type", meta::r#type::Type::new)
			.register_scalar("is::defined", is::defined::Defined::new)
			.register_scalar("is::undefined", is::undefined::IsUndefined::new)
			.register_scalar("is::type", is::r#type::IsType::new)
			.register_generator("generate_series", series::GenerateSeries::new)
			.register_generator("inspect_subscription", subscription::inspect::InspectSubscription::new);

		let functions = if let Some(configurator) = self.functions_configurator {
			configurator(default_builder).build()
		} else {
			default_builder.build()
		};

		// Create engine before CDC worker (CDC worker needs engine for cleanup)
		let engine = StandardEngine::new(
			multi.clone(),
			single.clone(),
			eventbus.clone(),
			Box::new(self.interceptors.build()),
			Catalog::new(catalog, schema_registry),
			runtime.clock().clone(),
			functions,
			self.ioc.clone(),
		);

		self.ioc = self.ioc.register(engine.clone());

		// Spawn CDC producer actor and register event listener
		// The handle is stored in IoC to keep it alive for the database lifetime
		// Engine is passed for periodic cleanup based on consumer watermarks
		let cdc_handle = spawn_cdc_producer(
			&runtime.actor_system(),
			cdc_store,
			multi_store,
			engine.clone(),
			eventbus.clone(),
		);
		eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
			cdc_handle.actor_ref().clone(),
			runtime.clock().clone(),
		));
		self.ioc.register_service::<Arc<reifydb_runtime::actor::system::ActorHandle<reifydb_cdc::produce::producer::CdcProduceMsg>>>(Arc::new(cdc_handle));

		// Collect all versions
		let mut all_versions = Vec::new();
		all_versions.push(SystemVersion {
			name: "reifydb".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "ReifyDB Database System".to_string(),
			r#type: ComponentType::Package,
		});

		all_versions.push(CoreVersion.version());
		all_versions.push(EngineVersion.version());
		all_versions.push(CatalogVersion.version());
		all_versions.push(MultiStoreVersion.version());
		all_versions.push(SingleStoreVersion.version());
		all_versions.push(TransactionVersion.version());
		all_versions.push(AuthVersion.version());
		all_versions.push(RqlVersion.version());
		all_versions.push(CdcVersion.version());

		// Create subsystems from factories and collect their versions
		// IMPORTANT: Order matters for shutdown! Subsystems are stopped in REVERSE order.
		// Add logging FIRST so it's stopped LAST and can log shutdown messages from other subsystems.
		let health_monitor = Arc::new(HealthMonitor::new());
		let mut subsystems = Subsystems::new(Arc::clone(&health_monitor));

		#[cfg(feature = "sub_tracing")]
		if let Some(factory) = self.tracing_factory {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(factory) = self.flow_factory {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		{
			let factory = self.task_factory.unwrap_or_else(|| {
				Box::new(TaskSubsystemFactory::with_config(TaskConfig::new(create_system_tasks())))
			});
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		for factory in self.factories {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		if let Some(git_hash) = option_env!("GIT_HASH") {
			all_versions.push(SystemVersion {
				name: "git-hash".to_string(),
				version: git_hash.to_string(),
				description: "Git commit hash at build time".to_string(),
				r#type: ComponentType::Build,
			});
		}

		let system_catalog = SystemCatalog::new(all_versions);
		self.ioc.register(system_catalog);

		Ok(Database::new(engine, subsystems, health_monitor, runtime))
	}

	/// Load the materialized catalog from storage
	fn load_materialized_catalog(
		multi: &MultiTransaction,
		single: &SingleTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone());

		debug!("Loading materialized catalog");
		MaterializedCatalogLoader::load_all(&mut qt, catalog)?;

		Ok(())
	}

	/// Load the schema registry from storage
	fn load_schema_registry(
		multi: &MultiTransaction,
		single: &SingleTransaction,
		registry: &SchemaRegistry,
	) -> crate::Result<()> {
		let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone());

		debug!("Loading schema registry");
		SchemaRegistryLoader::load_all(&mut qt, registry)?;

		Ok(())
	}
}
