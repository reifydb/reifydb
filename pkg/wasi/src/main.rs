// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WASI bridge for ReifyDB test suite.
//!
//! Implements a JSON stdin/stdout protocol and runs under wasmtime.

use std::{
	collections::HashMap,
	error::Error,
	fmt::Write as FmtWrite,
	io::{BufRead, Write},
};

use reifydb_auth::AuthVersion;
use reifydb_catalog::{
	CatalogVersion,
	bootstrap::{
		bootstrap_config_defaults, bootstrap_root_identity, bootstrap_system_procedures,
		load_materialized_catalog, load_schema_registry,
	},
	catalog::Catalog,
	materialized::MaterializedCatalog,
	schema::SchemaRegistry,
	system::SystemCatalog,
};
use reifydb_cdc::{
	CdcVersion,
	produce::producer::{CdcProducerEventListener, spawn_cdc_producer},
	storage::CdcStore,
};
use reifydb_core::{
	CoreVersion,
	config::SystemConfig,
	event::{EventBus, transaction::PostCommitEvent},
	interface::version::{ComponentType, HasVersion, SystemVersion},
	util::ioc::IocContainer,
};
use reifydb_engine::{
	EngineVersion, engine::StandardEngine, procedure::registry::Procedures, transform::registry::Transforms,
};
use reifydb_function::registry::Functions;
use reifydb_rql::RqlVersion;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig, context::RuntimeContext};
use reifydb_store_multi::{
	MultiStore, MultiStoreVersion,
	config::{HotConfig, MultiStoreConfig},
	hot::storage::HotStorage,
};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_api::subsystem::Subsystem;
use reifydb_sub_flow::{builder::FlowBuilderConfig, subsystem::FlowSubsystem};
use reifydb_transaction::{
	TransactionVersion,
	interceptor::factory::InterceptorFactory,
	multi::transaction::{MultiTransaction, register_oracle_defaults},
	single::SingleTransaction,
};
use reifydb_type::{params::Params, value::identity::IdentityId};

struct Bridge {
	engine: StandardEngine,
	flow_subsystem: FlowSubsystem,
}

impl Bridge {
	fn new() -> Result<Self, Box<dyn Error>> {
		let runtime = SharedRuntime::from_config(
			SharedRuntimeConfig::default()
				.async_threads(1)
				.compute_threads(1)
				.compute_max_in_flight(8)
				.deterministic_testing(0),
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
		});
		let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());

		let single = SingleTransaction::new(single_store.clone(), eventbus.clone());
		let system_config = SystemConfig::new();
		register_oracle_defaults(&system_config);
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			actor_system.clone(),
			runtime.clock().clone(),
			system_config.clone(),
		)?;

		let mut ioc = IocContainer::new();
		let materialized_catalog = MaterializedCatalog::new(system_config);
		ioc = ioc.register(materialized_catalog.clone());
		ioc = ioc.register(runtime.clone());
		ioc = ioc.register(single_store.clone());

		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		let ioc_ref = ioc.clone();

		let schema_registry = SchemaRegistry::new(single.clone());

		load_materialized_catalog(&multi, &single, &materialized_catalog)?;
		bootstrap_root_identity(&multi, &single, &materialized_catalog, &eventbus)?;
		bootstrap_config_defaults(&multi, &single, &materialized_catalog, &eventbus)?;
		bootstrap_system_procedures(&multi, &single, &materialized_catalog, &schema_registry, &eventbus)?;
		load_schema_registry(&multi, &single, &schema_registry)?;

		let procedures = Procedures::defaults().build();

		let eventbus_clone = eventbus.clone();
		let engine = StandardEngine::new(
			multi,
			single.clone(),
			eventbus,
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog, schema_registry),
			RuntimeContext::new(runtime.clock().clone(), runtime.rng().clone()),
			Functions::defaults().build(),
			procedures,
			Transforms::empty(),
			ioc,
			#[cfg(not(target_arch = "wasm32"))]
			None,
		);

		eprintln!("[WASI] Spawning CDC producer actor...");
		let cdc_producer_handle = spawn_cdc_producer(
			&actor_system,
			cdc_store,
			multi_store.clone(),
			engine.clone(),
			eventbus_clone.clone(),
		);

		let cdc_listener =
			CdcProducerEventListener::new(cdc_producer_handle.actor_ref().clone(), runtime.clock().clone());
		eventbus_clone.register::<PostCommitEvent, _>(cdc_listener);
		eprintln!("[WASI] CDC producer actor registered!");

		let flow_config = FlowBuilderConfig {
			operators_dir: None,
			num_workers: 1,
			custom_operators: HashMap::new(),
		};
		eprintln!("[WASI] Creating FlowSubsystem...");
		let mut flow_subsystem = FlowSubsystem::new(flow_config, engine.clone(), &ioc_ref);
		eprintln!("[WASI] Starting FlowSubsystem...");
		flow_subsystem.start()?;
		eprintln!("[WASI] FlowSubsystem started successfully!");

		let mut all_versions = Vec::new();
		all_versions.push(SystemVersion {
			name: "reifydb-wasi-bridge".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "ReifyDB WASI Bridge".to_string(),
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
		all_versions.push(flow_subsystem.version());

		ioc_ref.register_service(SystemCatalog::new(all_versions));

		Ok(Bridge {
			engine,
			flow_subsystem,
		})
	}
}

impl Drop for Bridge {
	fn drop(&mut self) {
		let _ = self.flow_subsystem.shutdown();
	}
}

fn respond(obj: &serde_json::Value) {
	let mut stdout = std::io::stdout().lock();
	let _ = serde_json::to_writer(&mut stdout, obj);
	let _ = stdout.write_all(b"\n");
	let _ = stdout.flush();
}

fn main() {
	let stdin = std::io::stdin();
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

		let msg: serde_json::Value = match serde_json::from_str(&line) {
			Ok(v) => v,
			Err(e) => {
				respond(&serde_json::json!({"err": format!("invalid JSON: {}", e)}));
				continue;
			}
		};

		// Fire any timers that expired while waiting for input (e.g. CDC poll timers).
		reifydb_runtime::actor::timers::drain_expired_timers();

		let cmd = msg.get("cmd").and_then(|v| v.as_str()).unwrap_or("");

		match cmd {
			"new" => match Bridge::new() {
				Ok(b) => {
					bridge = Some(b);
					respond(&serde_json::json!({"ok": "ready"}));
				}
				Err(e) => {
					respond(&serde_json::json!({"err": format!("{}", e)}));
				}
			},
			"command" => {
				let Some(b) = bridge.as_ref() else {
					respond(&serde_json::json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.command_as(IdentityId::root(), rql, Params::None) {
					Ok(frames) => {
						let mut output = String::new();
						for frame in &frames {
							let _ = writeln!(output, "{}", frame);
						}
						respond(&serde_json::json!({"ok": output}));
					}
					Err(e) => {
						respond(&serde_json::json!({"err": format!("{}", e)}));
					}
				}
			}
			"admin" => {
				let Some(b) = bridge.as_ref() else {
					respond(&serde_json::json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.admin_as(IdentityId::root(), rql, Params::None) {
					Ok(frames) => {
						let mut output = String::new();
						for frame in &frames {
							let _ = writeln!(output, "{}", frame);
						}
						respond(&serde_json::json!({"ok": output}));
					}
					Err(e) => {
						respond(&serde_json::json!({"err": format!("{}", e)}));
					}
				}
			}
			"query" => {
				let Some(b) = bridge.as_ref() else {
					respond(&serde_json::json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.query_as(IdentityId::root(), rql, Params::None) {
					Ok(frames) => {
						let mut output = String::new();
						for frame in &frames {
							let _ = writeln!(output, "{}", frame);
						}
						respond(&serde_json::json!({"ok": output}));
					}
					Err(e) => {
						respond(&serde_json::json!({"err": format!("{}", e)}));
					}
				}
			}
			"query_count" => {
				let Some(b) = bridge.as_ref() else {
					respond(&serde_json::json!({"err": "no database instance"}));
					continue;
				};
				let rql = msg.get("rql").and_then(|v| v.as_str()).unwrap_or("");
				match b.engine.query_as(IdentityId::root(), rql, Params::None) {
					Ok(frames) => {
						let count: usize = frames
							.iter()
							.flat_map(|f| f.columns.first())
							.map(|c| c.data.len())
							.sum();
						respond(&serde_json::json!({"ok": count.to_string()}));
					}
					Err(e) => {
						respond(&serde_json::json!({"err": format!("{}", e)}));
					}
				}
			}
			"free" => {
				bridge.take();
				respond(&serde_json::json!({"ok": "freed"}));
			}
			other => {
				respond(&serde_json::json!({"err": format!("unknown command: {}", other)}));
			}
		}
	}
}
