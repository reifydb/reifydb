// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WebAssembly bindings for ReifyDB query engine
//!
//! This crate provides JavaScript-compatible bindings for running ReifyDB
//! queries in a browser or Node.js environment with in-memory storage.

use reifydb_catalog::schema::SchemaRegistry;
use reifydb_engine::{engine::StandardEngine, procedure::registry::Procedures};
use wasm_bindgen::prelude::*;

// Debug helper to log to browser console
fn console_log(msg: &str) {
	web_sys::console::log_1(&msg.into());
}
use reifydb_cdc::{
	produce::producer::{CdcProducerEventListener, spawn_cdc_producer},
	storage::CdcStore,
};
use reifydb_core::{event::transaction::PostCommitEvent, interface::auth::Identity};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_sub_api::subsystem::Subsystem;
use reifydb_sub_flow::{builder::FlowBuilderConfig, subsystem::FlowSubsystem};
use reifydb_type::params::Params;

mod error;
mod utils;

pub use error::JsError;
use reifydb_function::registry::Functions;
use reifydb_store_multi::{
	config::{HotConfig, MultiStoreConfig},
	hot::storage::HotStorage,
};

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
		use reifydb_catalog::{catalog::Catalog, materialized::MaterializedCatalog};
		use reifydb_core::{event::EventBus, util::ioc::IocContainer};
		use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
		use reifydb_transaction::{
			interceptor::factory::InterceptorFactory, multi::transaction::MultiTransaction,
			single::SingleTransaction,
		};

		// Set panic hook for better error messages in browser console

		#[cfg(feature = "console_error_panic_hook")]
		console_error_panic_hook::set_once();

		// WASM runtime with minimal threads (single-threaded)
		let runtime = SharedRuntime::from_config(
			SharedRuntimeConfig::default().async_threads(1).compute_threads(1).compute_max_in_flight(8),
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
		let multi = MultiTransaction::new(
			multi_store.clone(),
			single.clone(),
			eventbus.clone(),
			actor_system.clone(),
			runtime.clock().clone(),
		)
		.map_err(|e| JsError::from_error(&e))?;

		// Setup IoC container
		let mut ioc = IocContainer::new();

		let materialized_catalog = MaterializedCatalog::new();
		ioc = ioc.register(materialized_catalog.clone());

		ioc = ioc.register(runtime.clone());

		// Register metrics store for engine
		ioc = ioc.register(single_store.clone());

		// Register CdcStore (required by sub-flow)
		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		// Clone ioc for FlowSubsystem (engine consumes ioc)
		let ioc_ref = ioc.clone();

		// Build engine first — CDC producer needs it as CdcHost for schema registry access
		let eventbus_clone = eventbus.clone();
		let inner = StandardEngine::new(
			multi,
			single.clone(),
			eventbus,
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog, SchemaRegistry::new(single)),
			runtime.clock().clone(),
			Functions::defaults().build(),
			Procedures::empty(),
			reifydb_engine::transform::registry::Transforms::empty(),
			ioc,
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
		};
		console_log("[WASM] Creating FlowSubsystem...");
		let mut flow_subsystem = FlowSubsystem::new(flow_config, inner.clone(), &ioc_ref);
		console_log("[WASM] Starting FlowSubsystem...");
		flow_subsystem.start().map_err(|e| JsError::from_error(&e))?;
		console_log("[WASM] FlowSubsystem started successfully!");

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
		let identity = Identity::root();
		let params = Params::None;

		// Execute query
		let frames = self.inner.query_as(&identity, rql, params).map_err(|e| JsError::from_error(&e))?;

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
		let identity = Identity::root();
		let params = Params::None;

		let frames = self.inner.admin_as(&identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute a command (DML) and return results
	///
	/// Commands include INSERT, UPDATE, DELETE, etc.
	/// For DDL operations (CREATE, ALTER), use `admin()` instead.
	#[wasm_bindgen]
	pub fn command(&self, rql: &str) -> Result<JsValue, JsValue> {
		let identity = Identity::root();
		let params = Params::None;

		let frames = self.inner.command_as(&identity, rql, params).map_err(|e| JsError::from_error(&e))?;

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
		let identity = Identity::root();

		// Parse JavaScript params to Rust Params
		let params = utils::parse_params(params_js)?;

		let frames = self.inner.query_as(&identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute admin with JSON parameters
	#[wasm_bindgen(js_name = adminWithParams)]
	pub fn admin_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = Identity::root();

		let params = utils::parse_params(params_js)?;

		let frames = self.inner.admin_as(&identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute command with JSON parameters
	#[wasm_bindgen(js_name = commandWithParams)]
	pub fn command_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = Identity::root();

		let params = utils::parse_params(params_js)?;

		let frames = self.inner.command_as(&identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
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
