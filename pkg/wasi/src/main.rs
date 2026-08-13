// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use std::{
	error::Error,
	fmt::Write as FmtWrite,
	io,
	io::{BufRead, Write},
	sync::Arc,
};

use reifydb_auth::AuthVersion;
use reifydb_catalog::{
	CatalogVersion,
	bootstrap::{bootstrap_system_objects, load_catalog_cache},
	cache::CatalogCache,
	catalog::Catalog,
	system::SystemCatalog,
};
use reifydb_cdc::{
	CdcVersion,
	consume::{backlog::FlowBacklog, wake::CdcWakeRegistry},
	produce::{
		producer::{CdcProducerEventListener, spawn_cdc_producer},
		watermark::CdcProducerWatermark,
	},
	storage::CdcStore,
};
use reifydb_core::{
	CoreVersion,
	event::{EventBus, transaction::PostCommitEvent},
	interface::{
		catalog::config::ConfigKey,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	lifecycle::metrics::RetentionMetrics,
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
use reifydb_engine::{EngineVersion, engine::StandardEngine, vm::services::EngineConfig};
use reifydb_extension::transform::registry::Transforms;
use reifydb_routine::{function::default_in_process_functions, procedure::default_in_process_procedures};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::RqlVersion;
use reifydb_runtime::{
	Runtime, RuntimeConfig,
	actor::timers::drain_expired_timers,
	context::{RuntimeContext, clock::Clock},
	pool::PoolConfig,
	shutdown::Shutdown,
	version_epoch::VersionEpoch,
};
use reifydb_store_multi::{
	MultiStore, MultiStoreVersion,
	config::{CommitBufferConfig, MultiStoreConfig},
	tier::commit::buffer::MultiCommitBufferTier,
};
use reifydb_store_operator::{OperatorStoreVersion, store::OperatorStore};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_flow::{builder::FlowConfig, subsystem::FlowSubsystem};
use reifydb_transaction::{
	TransactionVersion, interceptor::factory::InterceptorFactory, multi::transaction::MultiTransaction,
	single::SingleTransaction,
};
use reifydb_value::{byte_size::ByteSize, params::Params, value::identity::IdentityId};
use serde_json::{Value as JsonValue, from_str as json_from_str, json, to_writer as json_to_writer};

enum BridgeProfile {
	Default,
	Testing,
}

struct Bridge {
	engine: StandardEngine,
	flow_subsystem: FlowSubsystem,
	profile: BridgeProfile,
	_runtime: Runtime,
}

impl Bridge {
	fn new(profile: BridgeProfile) -> Result<Self, Box<dyn Error>> {
		let runtime = Runtime::from_config(
			RuntimeConfig::default().seeded(0),
			PoolConfig {
				coordination_threads: 1,
				flow_threads: 1,
				maintenance_threads: 1,
				task_threads: 1,
				compute_threads: 1,
				async_threads: 1,
			},
		);

		let spawner = runtime.spawner();
		let clock = runtime.clock().clone();
		let rng = runtime.rng().clone();
		let eventbus = EventBus::new(&spawner);

		let multi_store = MultiStore::standard(MultiStoreConfig {
			commit: CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			},
			persistent: None,
			read: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus: eventbus.clone(),
			spawner: spawner.clone(),
			clock: Clock::Real,
		});
		let single_store = SingleStore::testing_memory();
		let operator_store = OperatorStore::memory();
		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let catalog_cache = CatalogCache::new();
		let version_epoch = VersionEpoch::new();
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			spawner.clone(),
			clock.clone(),
			version_epoch.clone(),
			rng.clone(),
			Arc::new(catalog_cache.clone()),
		)?;

		let mut ioc = IocContainer::new();
		ioc = ioc.register(catalog_cache.clone());
		ioc = ioc.register(spawner.clone()).register(clock.clone()).register(rng.clone());
		ioc = ioc.register(MetricsRegistry::new());
		ioc = ioc.register(single_store.clone());
		ioc = ioc.register(operator_store.clone());

		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		let cdc_producer_watermark = CdcProducerWatermark::new();
		ioc = ioc.register(cdc_producer_watermark.clone());

		let cdc_wake_registry = CdcWakeRegistry::new();
		ioc = ioc.register(cdc_wake_registry.clone());

		let flow_backlog = FlowBacklog::new(ByteSize::from_bytes(
			multi.config().get_config_uint8(ConfigKey::FlowBacklogMemoryLimit),
		));
		ioc = ioc.register(flow_backlog.clone());

		ioc = ioc.register(RetentionMetrics::new());

		let ioc_ref = ioc.clone();

		load_catalog_cache(&multi, &single, &catalog_cache)?;
		bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus)?;

		let routines = {
			let b = Routines::builder();
			let b = default_in_process_functions(b);
			default_in_process_procedures(b).configure()
		};

		let eventbus_clone = eventbus.clone();
		let engine = StandardEngine::new(
			multi,
			single.clone(),
			eventbus,
			InterceptorFactory::default(),
			Catalog::new(catalog_cache),
			EngineConfig {
				runtime_context: RuntimeContext::new(clock.clone(), rng.clone(), version_epoch.clone()),
				routines,
				transforms: Transforms::empty(),
				ioc,
				#[cfg(not(target_arch = "wasm32"))]
				remote_registry: None,
			},
		);

		eprintln!("[WASI] Spawning CDC producer actor...");
		let cdc_producer_handle = spawn_cdc_producer(
			&spawner,
			cdc_store,
			multi_store.clone(),
			eventbus_clone.clone(),
			cdc_producer_watermark,
			cdc_wake_registry,
			flow_backlog,
		);

		let cdc_listener =
			CdcProducerEventListener::new(cdc_producer_handle.actor_ref().clone(), clock.clone());
		eventbus_clone.register::<PostCommitEvent, _>(cdc_listener);
		eprintln!("[WASI] CDC producer actor registered!");

		let flow_config = FlowConfig {
			operators_dir: None,
			custom_operators: Default::default(),
		};
		eprintln!("[WASI] Creating FlowSubsystem...");
		FlowSubsystem::publish_operator_catalog(&flow_config, &engine);
		let flow_subsystem = FlowSubsystem::new(flow_config, engine.clone(), &ioc_ref)?;
		eprintln!("[WASI] FlowSubsystem started successfully!");

		let all_versions = vec![
			SystemVersion {
				name: "reifydb-wasi-bridge".to_string(),
				version: env!("CARGO_PKG_VERSION").to_string(),
				description: "ReifyDB WASI Bridge".to_string(),
				r#type: ComponentType::Package,
			},
			CoreVersion.version(),
			EngineVersion.version(),
			CatalogVersion.version(),
			MultiStoreVersion.version(),
			SingleStoreVersion.version(),
			OperatorStoreVersion.version(),
			TransactionVersion.version(),
			AuthVersion.version(),
			RqlVersion.version(),
			CdcVersion.version(),
			flow_subsystem.version(),
		];

		ioc_ref.register_service(SystemCatalog::new(all_versions));

		Ok(Bridge {
			engine,
			flow_subsystem,
			profile,
			_runtime: runtime,
		})
	}
}

impl Drop for Bridge {
	fn drop(&mut self) {
		self.flow_subsystem.shutdown();
	}
}

fn respond(obj: &JsonValue) {
	let mut stdout = io::stdout().lock();
	let _ = json_to_writer(&mut stdout, obj);
	let _ = stdout.write_all(b"\n");
	let _ = stdout.flush();
}

fn main() {
	let stdin = io::stdin();
	let reader = stdin.lock();
	let mut bridge: Option<Bridge> = None;

	for line in reader.lines() {
		let line = match line {
			Ok(l) => l,
			Err(_) => break,
		};
		if line.trim().is_empty() {
			continue;
		}

		let msg: JsonValue = match json_from_str(&line) {
			Ok(v) => v,
			Err(e) => {
				respond(&json!({"err": format!("invalid JSON: {}", e)}));
				continue;
			}
		};

		// Fire any timers that expired while waiting for input (e.g. CDC poll timers).
		drain_expired_timers();

		let cmd = msg.get("cmd").and_then(|v| v.as_str()).unwrap_or("");

		match cmd {
			"new" => {
				let profile = match msg.get("profile").and_then(|v| v.as_str()) {
					Some("testing") => BridgeProfile::Testing,
					_ => BridgeProfile::Default,
				};
				match Bridge::new(profile) {
					Ok(b) => {
						bridge = Some(b);
						respond(&json!({"ok": "ready"}));
					}
					Err(e) => {
						respond(&json!({"err": format!("{}", e)}));
					}
				}
			}
			"command" => {
				let Some(b) = bridge.as_ref() else {
					respond(&json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.command_as(IdentityId::root(), rql, Params::None).check() {
					Ok(result) => {
						let mut output = String::new();
						for mut frame in result.frames {
							if matches!(b.profile, BridgeProfile::Testing) {
								frame.system.set_created_at(Vec::new());
								frame.system.set_updated_at(Vec::new());
							}
							let _ = writeln!(output, "{}", frame);
						}
						respond(&json!({"ok": output}));
					}
					Err(e) => {
						respond(&json!({"err": format!("{}", e)}));
					}
				}
			}
			"admin" => {
				let Some(b) = bridge.as_ref() else {
					respond(&json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.admin_as(IdentityId::root(), rql, Params::None).check() {
					Ok(result) => {
						let mut output = String::new();
						for mut frame in result.frames {
							if matches!(b.profile, BridgeProfile::Testing) {
								frame.system.set_created_at(Vec::new());
								frame.system.set_updated_at(Vec::new());
							}
							let _ = writeln!(output, "{}", frame);
						}
						respond(&json!({"ok": output}));
					}
					Err(e) => {
						respond(&json!({"err": format!("{}", e)}));
					}
				}
			}
			"query" => {
				let Some(b) = bridge.as_ref() else {
					respond(&json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.query_as(IdentityId::root(), rql, Params::None).check() {
					Ok(result) => {
						let mut output = String::new();
						for mut frame in result.frames {
							if matches!(b.profile, BridgeProfile::Testing) {
								frame.system.set_created_at(Vec::new());
								frame.system.set_updated_at(Vec::new());
							}
							let _ = writeln!(output, "{}", frame);
						}
						respond(&json!({"ok": output}));
					}
					Err(e) => {
						respond(&json!({"err": format!("{}", e)}));
					}
				}
			}
			"query_count" => {
				let Some(b) = bridge.as_ref() else {
					respond(&json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.query_as(IdentityId::root(), rql, Params::None).check() {
					Ok(result) => {
						let count: usize = result
							.iter()
							.flat_map(|f| f.columns.first())
							.map(|c| c.data.len())
							.sum();
						respond(&json!({"ok": count.to_string()}));
					}
					Err(e) => {
						respond(&json!({"err": format!("{}", e)}));
					}
				}
			}
			"free" => {
				bridge.take();
				respond(&json!({"ok": "freed"}));
			}
			other => {
				respond(&json!({"err": format!("unknown command: {}", other)}));
			}
		}
	}
}
