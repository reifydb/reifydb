// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WebAssembly bindings for ReifyDB query engine
//!
//! This crate provides JavaScript-compatible bindings for running ReifyDB
//! queries in a browser or Node.js environment with in-memory storage.

use reifydb_catalog::schema::SchemaRegistry;
use wasm_bindgen::prelude::*;
use reifydb_engine::engine::StandardEngine;
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_core::interface::auth::Identity;
use reifydb_type::params::Params;

mod utils;
mod error;

pub use error::JsError;

/// WebAssembly ReifyDB Engine
///
/// Provides an in-memory query engine that runs entirely in the browser.
/// All data is stored in memory and lost when the page is closed.
#[wasm_bindgen]
pub struct WasmEngine {
	inner: StandardEngine,
}

#[wasm_bindgen]
impl WasmEngine {
	/// Create a new in-memory ReifyDB engine
	///
	/// # Example
	///
	/// ```javascript
	/// import init, { WasmEngine } from './pkg/reifydb_engine_wasm.js';
	///
	/// await init();
	/// const engine = new WasmEngine();
	/// ```
	#[wasm_bindgen(constructor)]
	pub fn new() -> Result<WasmEngine, JsValue> {
		use reifydb_core::{event::EventBus, util::ioc::IocContainer};
		use reifydb_catalog::{catalog::Catalog, materialized::MaterializedCatalog};
		use reifydb_transaction::{
			interceptor::factory::StandardInterceptorFactory,
			multi::transaction::TransactionMulti,
			single::TransactionSingle,
		};
		use reifydb_rqlv2::compiler::Compiler;
		use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};

		// Set panic hook for better error messages in browser console

		#[cfg(feature = "console_error_panic_hook")]
		console_error_panic_hook::set_once();

		// Create event bus and stores
		let eventbus = EventBus::new();
		let multi_store = MultiStore::testing_memory_with_eventbus(eventbus.clone());
		let single_store = SingleStore::testing_memory_with_eventbus(eventbus.clone());

		// Create transactions
		let single = TransactionSingle::svl(single_store.clone(), eventbus.clone());
		let multi = TransactionMulti::new(multi_store.clone(), single.clone(), eventbus.clone())
			.map_err(|e| JsError::from_error(&e))?;

		// Setup IoC container
		let mut ioc = IocContainer::new();

		let materialized_catalog = MaterializedCatalog::new();
		ioc = ioc.register(materialized_catalog.clone());

		// WASM runtime with minimal threads (single-threaded)
		let runtime = SharedRuntime::from_config(
			SharedRuntimeConfig::default().async_threads(1).compute_threads(1).compute_max_in_flight(8),
		);
		ioc = ioc.register(runtime.clone());

		let compiler = Compiler::new(materialized_catalog.clone());
		ioc = ioc.register(compiler);

		// Register metrics store for engine
		ioc = ioc.register(single_store.clone());

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

		Ok(WasmEngine { inner })
	}

	/// Execute a query and return results as JavaScript objects
	///
	/// # Example
	///
	/// ```javascript
	/// const results = await engine.query(`
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
		let frames = self.inner.query_as(&identity, rql, params)
			.map_err(|e| JsError::from_error(&e))?;

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
	/// await engine.command("CREATE NAMESPACE demo");
	/// await engine.command(`
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

		let frames = self.inner.command_as(&identity, rql, params)
			.map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute query with JSON parameters
	///
	/// # Example
	///
	/// ```javascript
	/// const results = await engine.queryWithParams(
	///   "FROM users FILTER age > $min_age",
	///   { min_age: 25 }
	/// );
	/// ```
	#[wasm_bindgen(js_name = queryWithParams)]
	pub fn query_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = Identity::root();

		// Parse JavaScript params to Rust Params
		let params = utils::parse_params(params_js)?;

		let frames = self.inner.query_as(&identity, rql, params)
			.map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute command with JSON parameters
	#[wasm_bindgen(js_name = commandWithParams)]
	pub fn command_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = Identity::root();

		let params = utils::parse_params(params_js)?;

		let frames = self.inner.command_as(&identity, rql, params)
			.map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}
}

impl Default for WasmEngine {
	fn default() -> Self {
		Self::new().expect("Failed to create WasmEngine")
	}
}
