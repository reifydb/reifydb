// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WebAssembly bindings for ReifyDB query engine
//!
//! This crate provides JavaScript-compatible bindings for running ReifyDB
//! queries in a browser or Node.js environment with in-memory storage.

use reifydb_catalog::schema::SchemaRegistry;
use reifydb_engine::engine::StandardEngine;
use wasm_bindgen::prelude::*;

// Debug helper to log to browser console
fn console_log(msg: &str) {
	web_sys::console::log_1(&msg.into());
}
use reifydb_cdc::{
	produce::actor::{CdcProducerEventListener, spawn_cdc_producer},
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
		use reifydb_rqlv2::compiler::Compiler;
		use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
		use reifydb_transaction::{
			interceptor::factory::StandardInterceptorFactory, multi::transaction::TransactionMulti,
			single::TransactionSingle,
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
		let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
		let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());

		// Create transactions
		let single = TransactionSingle::svl(single_store.clone(), eventbus.clone());
		let multi = TransactionMulti::new(
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

		let compiler = Compiler::new(materialized_catalog.clone());
		ioc = ioc.register(compiler);

		// Register metrics store for engine
		ioc = ioc.register(single_store.clone());

		// Register CdcStore (required by sub-flow)
		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		// Spawn CDC producer actor on the shared runtime
		console_log("[WASM] Spawning CDC producer actor...");
		let cdc_producer_handle = spawn_cdc_producer(&actor_system, cdc_store, multi_store.clone());

		// Register event listener to forward PostCommitEvent to CDC producer
		let cdc_listener =
			CdcProducerEventListener::new(cdc_producer_handle.actor_ref().clone(), runtime.clock().clone());
		eventbus.register::<PostCommitEvent, _>(cdc_listener);
		console_log("[WASM] CDC producer actor registered!");

		// Clone ioc for FlowSubsystem (engine consumes ioc)
		let ioc_ref = ioc.clone();

		// Build engine
		let inner = StandardEngine::new(
			multi,
			single.clone(),
			eventbus,
			Box::new(StandardInterceptorFactory::default()),
			Catalog::new(materialized_catalog, SchemaRegistry::new(single)),
			None,
			ioc,
		);

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

	/// Execute a command (DDL/DML) and return results
	///
	/// Commands include CREATE, INSERT, UPDATE, DELETE, etc.
	///
	/// # Example
	///
	/// ```javascript
	/// await db.command("CREATE NAMESPACE demo");
	/// await db.command(`
	///   CREATE TABLE demo.users {
	///     id: int4,
	///     name: utf8
	///   }
	/// `);
	/// ```
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
