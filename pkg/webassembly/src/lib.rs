// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

//! WebAssembly bindings for ReifyDB query engine
//!
//! This crate provides JavaScript-compatible bindings for running ReifyDB
//! queries in a browser or Node.js environment with in-memory storage.

use std::{
	cell::{Cell, RefCell},
	collections::HashMap,
	fmt::Write,
	sync::Arc,
};

use reifydb_auth::{
	AuthVersion,
	registry::AuthenticationRegistry,
	service::{AuthResponse, AuthService, AuthServiceConfig},
};
use reifydb_catalog::{
	CatalogVersion,
	bootstrap::{bootstrap_configaults, bootstrap_system_procedures, load_materialized_catalog},
	catalog::Catalog,
	materialized::MaterializedCatalog,
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
use reifydb_engine::{EngineVersion, engine::StandardEngine, vm::services::EngineConfig};
use reifydb_routine::{function::default_functions, procedure::default_procedures};
use reifydb_rql::RqlVersion;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig, context::clock::Clock};
use reifydb_store_multi::{
	MultiStore, MultiStoreVersion,
	config::{HotConfig, MultiStoreConfig},
	hot::storage::HotStorage,
};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_api::subsystem::Subsystem;
use reifydb_sub_flow::{builder::FlowConfig, subsystem::FlowSubsystem};
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
use reifydb_extension::transform::registry::Transforms;
use reifydb_runtime::context::RuntimeContext;

/// Result of a successful login, returned to JavaScript.
#[wasm_bindgen]
pub struct LoginResult {
	token: String,
	identity: String,
}

#[wasm_bindgen]
impl LoginResult {
	#[wasm_bindgen(getter)]
	pub fn token(&self) -> String {
		self.token.clone()
	}

	#[wasm_bindgen(getter)]
	pub fn identity(&self) -> String {
		self.identity.clone()
	}
}

// Debug helper to log to browser console
fn console_log(msg: &str) {
	web_sys::console::log_1(&msg.into());
}

/// WebAssembly ReifyDB Engine
///
/// Provides an in-memory query engine that runs entirely in the browser.
/// All data is stored in memory and lost when the page is closed.
struct WasmSession {
	token: RefCell<Option<String>>,
	identity: Cell<Option<IdentityId>>,
}

impl WasmSession {
	fn new() -> Self {
		Self {
			token: RefCell::new(None),
			identity: Cell::new(None),
		}
	}

	fn current_identity(&self) -> IdentityId {
		self.identity.get().unwrap_or_else(IdentityId::root)
	}

	fn set(&self, identity: IdentityId, token: String) {
		self.identity.set(Some(identity));
		*self.token.borrow_mut() = Some(token);
	}

	fn clear(&self) {
		self.identity.set(None);
		*self.token.borrow_mut() = None;
	}

	fn take_token(&self) -> Option<String> {
		self.token.borrow().clone()
	}
}

#[wasm_bindgen]
pub struct WasmDB {
	inner: StandardEngine,
	flow_subsystem: FlowSubsystem,
	auth_service: AuthService,
	session: WasmSession,
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
			SharedRuntimeConfig::default().async_threads(1).compute_threads(1).deterministic_testing(0),
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
			clock: Clock::Real,
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
			runtime.rng().clone(),
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

		// Run shared bootstrap: load catalog, config defaults, system procedures, shapes
		load_materialized_catalog(&multi, &single, &materialized_catalog)
			.map_err(|e| JsError::from_error(&e))?;
		bootstrap_configaults(&multi, &single, &materialized_catalog, &eventbus)
			.map_err(|e| JsError::from_error(&e))?;
		bootstrap_system_procedures(&multi, &single, &materialized_catalog, &eventbus)
			.map_err(|e| JsError::from_error(&e))?;

		let procedures = default_procedures().configure();

		// Build engine with bootstrap-initialized catalog
		let eventbus_clone = eventbus.clone();
		let inner = StandardEngine::new(
			multi,
			single.clone(),
			eventbus,
			InterceptorFactory::default(),
			Catalog::new(materialized_catalog),
			EngineConfig {
				runtime_context: RuntimeContext::new(runtime.clock().clone(), runtime.rng().clone()),
				functions: default_functions().configure(),
				procedures,
				transforms: Transforms::empty(),
				ioc,
				#[cfg(not(target_arch = "wasm32"))]
				remote_registry: None,
			},
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
		let flow_config = FlowConfig {
			operators_dir: None, // No FFI operators in WASM
			num_workers: 1,      // Single-threaded for WASM
			custom_operators: HashMap::new(),
			connector_registry: Default::default(),
		};
		console_log("[WASM] Creating FlowSubsystem...");
		let mut flow_subsystem = FlowSubsystem::new(flow_config, inner.clone(), &ioc_ref);
		console_log("[WASM] Starting FlowSubsystem...");
		flow_subsystem.start().map_err(|e| JsError::from_error(&e))?;
		console_log("[WASM] FlowSubsystem started successfully!");

		// Collect all versions and register SystemCatalog
		let all_versions = vec![
			SystemVersion {
				name: "reifydb-webassembly".to_string(),
				version: env!("CARGO_PKG_VERSION").to_string(),
				description: "ReifyDB WebAssembly Engine".to_string(),
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

		let auth_service = AuthService::new(
			Arc::new(inner.clone()),
			Arc::new(AuthenticationRegistry::new(runtime.clock().clone())),
			runtime.rng().clone(),
			runtime.clock().clone(),
			AuthServiceConfig::default(),
		);

		Ok(WasmDB {
			inner,
			flow_subsystem,
			auth_service,
			session: WasmSession::new(),
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
		let identity = self.session.current_identity();
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
		let identity = self.session.current_identity();
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
		let identity = self.session.current_identity();
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
		let identity = self.session.current_identity();

		// Parse JavaScript params to Rust Params
		let params = utils::parse_params(params_js)?;

		let frames = self.inner.query_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute admin with JSON parameters
	#[wasm_bindgen(js_name = adminWithParams)]
	pub fn admin_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();

		let params = utils::parse_params(params_js)?;

		let frames = self.inner.admin_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute command with JSON parameters
	#[wasm_bindgen(js_name = commandWithParams)]
	pub fn command_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();

		let params = utils::parse_params(params_js)?;

		let frames = self.inner.command_as(identity, rql, params).map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&frames)
	}

	/// Execute a command and return Display-formatted text output
	#[wasm_bindgen(js_name = commandText)]
	pub fn command_text(&self, rql: &str) -> Result<String, JsValue> {
		let frames = self
			.inner
			.command_as(self.session.current_identity(), rql, Params::None)
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in &frames {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_message(&e.to_string()))?;
		}
		Ok(output)
	}

	/// Execute an admin operation and return Display-formatted text output
	#[wasm_bindgen(js_name = adminText)]
	pub fn admin_text(&self, rql: &str) -> Result<String, JsValue> {
		let frames = self
			.inner
			.admin_as(self.session.current_identity(), rql, Params::None)
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in &frames {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_message(&e.to_string()))?;
		}
		Ok(output)
	}

	/// Execute a query and return Display-formatted text output
	#[wasm_bindgen(js_name = queryText)]
	pub fn query_text(&self, rql: &str) -> Result<String, JsValue> {
		let frames = self
			.inner
			.query_as(self.session.current_identity(), rql, Params::None)
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in &frames {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_message(&e.to_string()))?;
		}
		Ok(output)
	}

	/// Authenticate with a password and return a session token.
	#[wasm_bindgen(js_name = loginWithPassword)]
	pub fn login_with_password(&self, identifier: &str, password: &str) -> Result<LoginResult, JsValue> {
		let mut credentials = HashMap::new();
		credentials.insert("identifier".to_string(), identifier.to_string());
		credentials.insert("password".to_string(), password.to_string());

		let response =
			self.auth_service.authenticate("password", credentials).map_err(|e| JsError::from_error(&e))?;

		self.handle_auth_response(response)
	}

	/// Authenticate with a token credential and return a session token.
	#[wasm_bindgen(js_name = loginWithToken)]
	pub fn login_with_token(&self, token: &str) -> Result<LoginResult, JsValue> {
		let mut credentials = HashMap::new();
		credentials.insert("token".to_string(), token.to_string());

		let response =
			self.auth_service.authenticate("token", credentials).map_err(|e| JsError::from_error(&e))?;

		self.handle_auth_response(response)
	}

	/// Logout and revoke the current session token.
	#[wasm_bindgen]
	pub fn logout(&self) -> Result<(), JsValue> {
		let token = self.session.take_token();
		match token {
			Some(t) => {
				let revoked = self.auth_service.revoke_token(&t);
				self.session.clear();
				if revoked {
					Ok(())
				} else {
					Err(JsError::from_message("Failed to revoke session token"))
				}
			}
			None => Ok(()),
		}
	}
}

impl WasmDB {
	fn handle_auth_response(&self, response: AuthResponse) -> Result<LoginResult, JsValue> {
		match response {
			AuthResponse::Authenticated {
				identity,
				token,
			} => {
				self.session.set(identity, token.clone());
				Ok(LoginResult {
					token,
					identity: identity.to_string(),
				})
			}
			AuthResponse::Failed {
				reason,
			} => Err(JsError::from_message(&format!("Authentication failed: {}", reason))),
			AuthResponse::Challenge {
				..
			} => Err(JsError::from_message(
				"Challenge-response authentication is not supported in WASM mode",
			)),
		}
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
