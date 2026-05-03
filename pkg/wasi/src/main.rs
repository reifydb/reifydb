// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use std::{
	collections::HashMap,
	error::Error,
	fmt::Write as FmtWrite,
	io,
	io::{BufRead, Write},
	sync::Arc,
};

use reifydb_auth::AuthVersion;
use reifydb_catalog::{
	CatalogVersion,
	bootstrap::{bootstrap_system_objects, load_materialized_catalog},
	catalog::Catalog,
	materialized::MaterializedCatalog,
	system::SystemCatalog,
};
use reifydb_cdc::{
	CdcVersion,
	produce::{
		producer::{CdcProducerEventListener, spawn_cdc_producer},
		watermark::CdcProducerWatermark,
	},
	storage::CdcStore,
};
use reifydb_core::{
	CoreVersion,
	event::{EventBus, transaction::PostCommitEvent},
	interface::version::{ComponentType, HasVersion, SystemVersion},
	util::ioc::IocContainer,
};
use reifydb_engine::{EngineVersion, engine::StandardEngine, vm::services::EngineConfig};
use reifydb_extension::transform::registry::Transforms;
use reifydb_routine::{
	function::default_native_functions, procedure::default_native_procedures, routine::registry::Routines,
};
use reifydb_rql::RqlVersion;
use reifydb_runtime::{
	SharedRuntime, SharedRuntimeConfig,
	actor::timers::drain_expired_timers,
	context::{RuntimeContext, clock::Clock},
	pool::PoolConfig,
};
use reifydb_store_multi::{
	MultiStore, MultiStoreVersion,
	config::{HotConfig, MultiStoreConfig},
	hot::storage::HotStorage,
};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_api::subsystem::Subsystem;
use reifydb_sub_flow::{builder::FlowConfig, subsystem::FlowSubsystem};
use reifydb_transaction::{
	TransactionVersion, interceptor::factory::InterceptorFactory, multi::transaction::MultiTransaction,
	single::SingleTransaction,
};
use reifydb_type::{params::Params, value::identity::IdentityId};
use serde_json::{Value as JsonValue, from_str as json_from_str, json, to_writer as json_to_writer};

enum BridgeProfile {
	Default,
	Testing,
}

struct Bridge {
	engine: StandardEngine,
	flow_subsystem: FlowSubsystem,
	profile: BridgeProfile,
}

impl Bridge {
	fn new(profile: BridgeProfile) -> Result<Self, Box<dyn Error>> {
		let runtime = SharedRuntime::from_config(
			SharedRuntimeConfig::default().seeded(0),
			PoolConfig {
				async_threads: 1,
				system_threads: 1,
				query_threads: 1,
			},
		);

		let actor_system = runtime.actor_system();
		let eventbus = EventBus::new(&actor_system);

		let multi_store = MultiStore::standard(MultiStoreConfig {
			hot: Some(HotConfig {
				storage: HotStorage::memory(),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus: eventbus.clone(),
			actor_system: actor_system.clone(),
			clock: Clock::Real,
		});
		let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());
		// Create transactions
		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let materialized_catalog = MaterializedCatalog::new();
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			actor_system.clone(),
			runtime.clock().clone(),
			runtime.rng().clone(),
			Arc::new(materialized_catalog.clone()),
		)?;

		let mut ioc = IocContainer::new();
		ioc = ioc.register(materialized_catalog.clone());
		ioc = ioc.register(runtime.clone());
		ioc = ioc.register(single_store.clone());

		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		let cdc_producer_watermark = CdcProducerWatermark::new();
		ioc = ioc.register(cdc_producer_watermark.clone());

		let ioc_ref = ioc.clone();

		load_materialized_catalog(&multi, &single, &materialized_catalog)?;
		bootstrap_system_objects(&multi, &single, &materialized_catalog, &eventbus)?;

		let routines = {
			let b = Routines::builder();
			let b = default_native_functions(b);
			default_native_procedures(b).configure()
		};

		let eventbus_clone = eventbus.clone();
		let engine = StandardEngine::new(
			multi,
			single.clone(),
			eventbus,
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog),
			EngineConfig {
				runtime_context: RuntimeContext::new(runtime.clock().clone(), runtime.rng().clone()),
				routines,
				transforms: Transforms::empty(),
				ioc,
				#[cfg(not(target_arch = "wasm32"))]
				remote_registry: None,
			},
		);

		eprintln!("[WASI] Spawning CDC producer actor...");
		let cdc_producer_handle = spawn_cdc_producer(
			&actor_system,
			cdc_store,
			multi_store.clone(),
			engine.clone(),
			eventbus_clone.clone(),
			runtime.clock().clone(),
			cdc_producer_watermark,
		);

		let cdc_listener =
			CdcProducerEventListener::new(cdc_producer_handle.actor_ref().clone(), runtime.clock().clone());
		eventbus_clone.register::<PostCommitEvent, _>(cdc_listener);
		eprintln!("[WASI] CDC producer actor registered!");

		let flow_config = FlowConfig {
			operators_dir: None,
			custom_operators: HashMap::new(),
			connector_registry: Default::default(),
		};
		eprintln!("[WASI] Creating FlowSubsystem...");
		let mut flow_subsystem = FlowSubsystem::new(flow_config, engine.clone(), &ioc_ref);
		eprintln!("[WASI] Starting FlowSubsystem...");
		flow_subsystem.start()?;
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
		})
	}
}

impl Drop for Bridge {
	fn drop(&mut self) {
		let _ = self.flow_subsystem.shutdown();
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
								frame.created_at.clear();
								frame.updated_at.clear();
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
								frame.created_at.clear();
								frame.updated_at.clear();
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
								frame.created_at.clear();
								frame.updated_at.clear();
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
