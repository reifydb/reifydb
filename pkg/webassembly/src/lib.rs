// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WebAssembly bindings for ReifyDB query engine
//!
//! This crate provides JavaScript-compatible bindings for running ReifyDB
//! queries in a browser or Node.js environment with in-memory storage.

use std::{collections::HashMap, fmt::Write};

use reifydb_auth::AuthVersion;
use reifydb_catalog::{
	CatalogVersion,
	bootstrap::{
		bootstrap_config_defaults, bootstrap_system_procedures, load_materialized_catalog, load_schema_registry,
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
	EngineVersion,
	engine::StandardEngine,
	procedure::{registry::Procedures, system::set_config::SetConfigProcedure},
};
use reifydb_function::registry::Functions;
use reifydb_rql::RqlVersion;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
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
use wasm_bindgen::prelude::*;

mod error;
mod utils;

pub use error::JsError;

// Debug helper to log to browser console
fn console_log(msg: &str) {
	web_sys::console::log_1(&msg.into());
}

/// WebAssembly ReifyDB Engine
///
/// Provides an in-memory query engine that runs entirely in the browser.
/// All data is stored in memory and lost when the page is closed.
#[wasm_bindgen]
pub struct WasmDB {
	inner: StandardEngine,
	flow_subsystem: FlowSubsystem,
}

#[wasm_bindgen]
impl WasmDB {
	/// Create a new in-memory ReifyDB engine
	///
	/// # Example
	///
	/// ```javascript
	/// import init, { WasmDB } from './pkg/reifydb_engine_wasm.js';
	///
	/// await init();
	/// const db = new WasmDB();
	/// ```
	#[wasm_bindgen(constructor)]
	pub fn new() -> Result<WasmDB, JsValue> {
		// Set panic hook for better error messages in browser console

		#[cfg(feature = "console_error_panic_hook")]
		console_error_panic_hook::set_once();

		// WASM runtime with minimal threads (single-threaded)
		let runtime = SharedRuntime::from_config(
			SharedRuntimeConfig::default()
				.async_threads(1)
				.compute_threads(1)
				.compute_max_in_flight(8)
				.mock_clock(0),
		);

		// Create actor system at the top level - this will be shared by
		// the transaction manager (watermark actors) and flow subsystem (poll/coordinator actors)
		let actor_system = runtime.actor_system();

		// Create event bus and stores
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

		// Create transactions
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
		)
		.map_err(|e| JsError::from_error(&e))?;

		// Setup IoC container
		let mut ioc = IocContainer::new();

		let materialized_catalog = MaterializedCatalog::new(system_config);
		ioc = ioc.register(materialized_catalog.clone());

		ioc = ioc.register(runtime.clone());

		// Register metrics store for engine
		ioc = ioc.register(single_store.clone());

		// Register CdcStore (required by sub-flow)
		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		// Clone ioc for FlowSubsystem (engine consumes ioc)
		let ioc_ref = ioc.clone();

		// Create SchemaRegistry for bootstrap
		let schema_registry = SchemaRegistry::new(single.clone());

		// Run shared bootstrap: load catalog, config defaults, system procedures, schemas
		load_materialized_catalog(&multi, &single, &materialized_catalog)
			.map_err(|e| JsError::from_error(&e))?;
		bootstrap_config_defaults(&multi, &single, &materialized_catalog, &eventbus)
			.map_err(|e| JsError::from_error(&e))?;
		bootstrap_system_procedures(&multi, &single, &materialized_catalog, &schema_registry, &eventbus)
			.map_err(|e| JsError::from_error(&e))?;
		load_schema_registry(&multi, &single, &schema_registry).map_err(|e| JsError::from_error(&e))?;

		// Build procedures with system::config::set native procedure
		let procedures =
			Procedures::builder().with_procedure("system::config::set", SetConfigProcedure::new).build();

		// Build engine with bootstrap-initialized catalog
		let eventbus_clone = eventbus.clone();
		let inner = StandardEngine::new(
			multi,
			single.clone(),
			eventbus,
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog, schema_registry),
			runtime.clock().clone(),
			Functions::defaults().build(),
			procedures,
			reifydb_engine::transform::registry::Transforms::empty(),
			ioc,
			#[cfg(not(target_arch = "wasm32"))]
			None,
		);

		// Spawn CDC producer actor on the shared runtime, passing engine as CdcHost
		console_log("[WASM] Spawning CDC producer actor...");
		let cdc_producer_handle = spawn_cdc_producer(
			&actor_system,
			cdc_store,
			multi_store.clone(),
			inner.clone(),
			eventbus_clone.clone(),
		);

		// Register event listener to forward PostCommitEvent to CDC producer
		let cdc_listener =
			CdcProducerEventListener::new(cdc_producer_handle.actor_ref().clone(), runtime.clock().clone());
		eventbus_clone.register::<PostCommitEvent, _>(cdc_listener);
		console_log("[WASM] CDC producer actor registered!");

		// Create and start FlowSubsystem
		let flow_config = FlowBuilderConfig {
			operators_dir: None, // No FFI operators in WASM
			num_workers: 1,      // Single-threaded for WASM
			custom_operators: HashMap::new(),
		};
		console_log("[WASM] Creating FlowSubsystem...");
		let mut flow_subsystem = FlowSubsystem::new(flow_config, inner.clone(), &ioc_ref);
		console_log("[WASM] Starting FlowSubsystem...");
		flow_subsystem.start().map_err(|e| JsError::from_error(&e))?;
		console_log("[WASM] FlowSubsystem started successfully!");

		// Collect all versions and register SystemCatalog
		let mut all_versions = Vec::new();
		all_versions.push(SystemVersion {
			name: "reifydb-webassembly".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "ReifyDB WebAssembly Engine".to_string(),
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

		Ok(WasmDB {
			inner,
			flow_subsystem,
		})
	}

	/// Execute a query and return results as JavaScript objects
	///
	/// # Example
	///
	/// ```javascript
	/// const results = await db.query(`
	///   FROM [{ name: "Alice", age: 30 }]
	///   FILTER age > 25
	/// `);
	/// console.log(results); // [{ name: "Alice", age: 30 }]
	/// ```
	#[wasm_bindgen]
	pub fn query(&self, rql: &str) -> Result<JsValue, JsValue> {
		let identity = IdentityId::root();
		let params = Params::None;

		// Execute query
		let frames = self.inner.query_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		// Convert frames to JavaScript array of objects
		utils::frames_to_js(&frames)
	}

	/// Execute an admin operation (DDL + DML + Query) and return results
	///
	/// Admin operations include CREATE, ALTER, INSERT, UPDATE, DELETE, etc.
	///
	/// # Example
	///
	/// ```javascript
	/// await db.admin("CREATE NAMESPACE demo");
	/// await db.admin(`
	///   CREATE TABLE demo.users {
	///     id: int4,
	///     name: utf8
	///   }
	/// `);
	/// ```
	#[wasm_bindgen]
	pub fn admin(&self, rql: &str) -> Result<JsValue, JsValue> {
		let identity = IdentityId::root();
		let params = Params::None;

		let frames = self.inner.admin_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute a command (DML) and return results
	///
	/// Commands include INSERT, UPDATE, DELETE, etc.
	/// For DDL operations (CREATE, ALTER), use `admin()` instead.
	#[wasm_bindgen]
	pub fn command(&self, rql: &str) -> Result<JsValue, JsValue> {
		let identity = IdentityId::root();
		let params = Params::None;

		let frames = self.inner.command_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute query with JSON parameters
	///
	/// # Example
	///
	/// ```javascript
	/// const results = await db.queryWithParams(
	///   "FROM users FILTER age > $min_age",
	///   { min_age: 25 }
	/// );
	/// ```
	#[wasm_bindgen(js_name = queryWithParams)]
	pub fn query_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = IdentityId::root();

		// Parse JavaScript params to Rust Params
		let params = utils::parse_params(params_js)?;

		let frames = self.inner.query_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute admin with JSON parameters
	#[wasm_bindgen(js_name = adminWithParams)]
	pub fn admin_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = IdentityId::root();

		let params = utils::parse_params(params_js)?;

		let frames = self.inner.admin_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute command with JSON parameters
	#[wasm_bindgen(js_name = commandWithParams)]
	pub fn command_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = IdentityId::root();

		let params = utils::parse_params(params_js)?;

		let frames = self.inner.command_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute a command and return Display-formatted text output
	#[wasm_bindgen(js_name = commandText)]
	pub fn command_text(&self, rql: &str) -> Result<String, JsValue> {
		let frames = self
			.inner
			.command_as(IdentityId::root(), rql, Params::None)
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in &frames {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_str(&e.to_string()))?;
		}
		Ok(output)
	}

	/// Execute an admin operation and return Display-formatted text output
	#[wasm_bindgen(js_name = adminText)]
	pub fn admin_text(&self, rql: &str) -> Result<String, JsValue> {
		let frames = self
			.inner
			.admin_as(IdentityId::root(), rql, Params::None)
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in &frames {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_str(&e.to_string()))?;
		}
		Ok(output)
	}

	/// Execute a query and return Display-formatted text output
	#[wasm_bindgen(js_name = queryText)]
	pub fn query_text(&self, rql: &str) -> Result<String, JsValue> {
		let frames = self
			.inner
			.query_as(IdentityId::root(), rql, Params::None)
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in &frames {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_str(&e.to_string()))?;
		}
		Ok(output)
	}
}

impl Drop for WasmDB {
	fn drop(&mut self) {
		let _ = self.flow_subsystem.shutdown();
	}
}

impl Default for WasmDB {
	fn default() -> Self {
		Self::new().expect("Failed to create WasmDB")
	}
}
