// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

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
	interface::version::{ComponentType, HasVersion, SystemVersion},
	lifecycle::metrics::RetentionMetrics,
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
use reifydb_engine::{EngineVersion, engine::StandardEngine, vm::services::EngineConfig};
use reifydb_routine::{function::default_in_process_functions, procedure::default_in_process_procedures};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::RqlVersion;
use reifydb_runtime::{
	Runtime, RuntimeConfig, context::clock::Clock, pool::PoolConfig, shutdown::Shutdown,
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
use reifydb_value::{params::Params, value::identity::IdentityId};
use wasm_bindgen::prelude::*;
use web_sys::console;

mod error;
mod utils;

#[cfg(feature = "console_error_panic_hook")]
use console_error_panic_hook::set_once as set_panic_hook;
pub use error::JsError;
use reifydb_codec::{
	frame::{decode::decode_frames, encode::encode_frames, format::Encoding, options::EncodeOptions},
	json::{from::frames_from_json, to::frames_to_json},
};
use reifydb_extension::transform::registry::Transforms;
use reifydb_runtime::context::RuntimeContext;

#[wasm_bindgen(js_name = encode_rbcf)]
pub fn encode_rbcf(frames_json: &str, forced_encoding: Option<String>) -> Result<Vec<u8>, JsValue> {
	let frames = frames_from_json(frames_json).map_err(|e| JsError::from_error(&e))?;
	let mut options = EncodeOptions::default();
	if let Some(enc_str) = forced_encoding {
		let enc = match enc_str.to_lowercase().as_str() {
			"plain" => Encoding::Plain,
			"dict" => Encoding::Dict,
			"rle" => Encoding::Rle,
			"delta" => Encoding::Delta,
			"deltarle" | "delta_rle" => Encoding::DeltaRle,
			_ => return Err(JsError::from_message(&format!("unknown encoding: {}", enc_str))),
		};
		options.force_encoding = Some(enc);
	}
	let bytes = encode_frames(&frames, &options).map_err(|e| JsError::from_error(&e))?;
	Ok(bytes)
}

#[wasm_bindgen(js_name = decode_rbcf)]
pub fn decode_rbcf(bytes: &[u8]) -> Result<String, JsValue> {
	let frames = decode_frames(bytes).map_err(|e| JsError::from_error(&e))?;
	let json = frames_to_json(&frames).map_err(|e| JsError::from_message(&e.to_string()))?;
	Ok(json)
}

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

fn console_log(msg: &str) {
	console::log_1(&msg.into());
}

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

/// Runs entirely in the browser; all data lives in memory and is lost when the page closes.
#[wasm_bindgen]
pub struct WasmDB {
	inner: StandardEngine,
	flow_subsystem: FlowSubsystem,
	auth_service: AuthService,
	session: WasmSession,
	_runtime: Runtime,
}

#[wasm_bindgen]
impl WasmDB {
	#[wasm_bindgen(constructor)]
	pub fn new() -> Result<WasmDB, JsValue> {
		#[cfg(feature = "console_error_panic_hook")]
		set_panic_hook();

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
		)
		.map_err(|e| JsError::from_error(&e))?;

		let mut ioc = IocContainer::new();

		ioc = ioc.register(catalog_cache.clone());

		ioc = ioc.register(spawner.clone()).register(clock.clone()).register(rng.clone());

		ioc = ioc.register(MetricsRegistry::new());

		ioc = ioc.register(single_store.clone());

		ioc = ioc.register(operator_store.clone());

		// Register CdcStore (required by sub-flow)
		let cdc_store = CdcStore::memory();
		ioc = ioc.register(cdc_store.clone());

		let cdc_producer_watermark = CdcProducerWatermark::new();
		ioc = ioc.register(cdc_producer_watermark.clone());

		let cdc_wake_registry = CdcWakeRegistry::new();
		ioc = ioc.register(cdc_wake_registry.clone());

		let flow_backlog = FlowBacklog::with_default_limit();
		ioc = ioc.register(flow_backlog.clone());

		// Register RetentionMetrics (required by FlowSubsystem)
		ioc = ioc.register(RetentionMetrics::new());

		// Clone ioc for FlowSubsystem (engine consumes ioc)
		let ioc_ref = ioc.clone();

		load_catalog_cache(&multi, &single, &catalog_cache).map_err(|e| JsError::from_error(&e))?;
		bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus)
			.map_err(|e| JsError::from_error(&e))?;

		let routines = {
			let b = Routines::builder();
			let b = default_in_process_functions(b);
			default_in_process_procedures(b).configure()
		};

		let eventbus_clone = eventbus.clone();
		let inner = StandardEngine::new(
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

		console_log("[WASM] Spawning CDC producer actor...");
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
		console_log("[WASM] CDC producer actor registered!");

		let flow_config = FlowConfig {
			operators_dir: None,
			custom_operators: Default::default(),
		};
		console_log("[WASM] Creating FlowSubsystem...");
		FlowSubsystem::publish_operator_catalog(&flow_config, &inner);
		let flow_subsystem = FlowSubsystem::new(flow_config, inner.clone(), &ioc_ref)
			.map_err(|e| JsError::from_error(&e))?;
		console_log("[WASM] FlowSubsystem started successfully!");

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
			OperatorStoreVersion.version(),
			TransactionVersion.version(),
			AuthVersion.version(),
			RqlVersion.version(),
			CdcVersion.version(),
			flow_subsystem.version(),
		];

		ioc_ref.register_service(SystemCatalog::new(all_versions));

		let auth_service = AuthService::new(
			Arc::new(inner.clone()),
			Arc::new(AuthenticationRegistry::new(clock.clone())),
			rng.clone(),
			clock.clone(),
			AuthServiceConfig::default(),
		);

		Ok(WasmDB {
			inner,
			flow_subsystem,
			auth_service,
			session: WasmSession::new(),
			_runtime: runtime,
		})
	}

	/// Read-only; results come back as an array of plain JavaScript objects.
	#[wasm_bindgen]
	pub fn query(&self, rql: &str) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();
		let params = Params::None;

		let result = self.inner.query_as(identity, rql, params).check().map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&result)
	}

	/// The only entry point that accepts DDL; also handles DML and queries.
	#[wasm_bindgen]
	pub fn admin(&self, rql: &str) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();
		let params = Params::None;

		let result = self.inner.admin_as(identity, rql, params).check().map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&result)
	}

	/// DML only; DDL must go through `admin()`.
	#[wasm_bindgen]
	pub fn command(&self, rql: &str) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();
		let params = Params::None;

		let result =
			self.inner.command_as(identity, rql, params).check().map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&result)
	}

	#[wasm_bindgen(js_name = queryWithParams)]
	pub fn query_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();

		let params = utils::parse_params(params_js)?;

		let result = self.inner.query_as(identity, rql, params).check().map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&result)
	}

	#[wasm_bindgen(js_name = adminWithParams)]
	pub fn admin_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();

		let params = utils::parse_params(params_js)?;

		let result = self.inner.admin_as(identity, rql, params).check().map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&result)
	}

	#[wasm_bindgen(js_name = commandWithParams)]
	pub fn command_with_params(&self, rql: &str, params_js: JsValue) -> Result<JsValue, JsValue> {
		let identity = self.session.current_identity();

		let params = utils::parse_params(params_js)?;

		let result =
			self.inner.command_as(identity, rql, params).check().map_err(|e| JsError::from_error(&e))?;

		utils::frames_to_js(&result)
	}

	/// Returns the Display-rendered frames rather than JavaScript objects.
	#[wasm_bindgen(js_name = commandText)]
	pub fn command_text(&self, rql: &str) -> Result<String, JsValue> {
		let result = self
			.inner
			.command_as(self.session.current_identity(), rql, Params::None)
			.check()
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in result.iter() {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_message(&e.to_string()))?;
		}
		Ok(output)
	}

	/// Returns the Display-rendered frames rather than JavaScript objects.
	#[wasm_bindgen(js_name = adminText)]
	pub fn admin_text(&self, rql: &str) -> Result<String, JsValue> {
		let result = self
			.inner
			.admin_as(self.session.current_identity(), rql, Params::None)
			.check()
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in result.iter() {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_message(&e.to_string()))?;
		}
		Ok(output)
	}

	/// Returns the Display-rendered frames rather than JavaScript objects.
	#[wasm_bindgen(js_name = queryText)]
	pub fn query_text(&self, rql: &str) -> Result<String, JsValue> {
		let result = self
			.inner
			.query_as(self.session.current_identity(), rql, Params::None)
			.check()
			.map_err(|e| JsError::from_error(&e))?;
		let mut output = String::new();
		for frame in result.iter() {
			writeln!(output, "{}", frame).map_err(|e| JsError::from_message(&e.to_string()))?;
		}
		Ok(output)
	}

	#[wasm_bindgen(js_name = loginWithPassword)]
	pub fn login_with_password(&self, identifier: &str, password: &str) -> Result<LoginResult, JsValue> {
		let mut credentials = HashMap::new();
		credentials.insert("identifier".to_string(), identifier.to_string());
		credentials.insert("password".to_string(), password.to_string());

		let response =
			self.auth_service.authenticate("password", credentials).map_err(|e| JsError::from_error(&e))?;

		self.handle_auth_response(response)
	}

	#[wasm_bindgen(js_name = loginWithToken)]
	pub fn login_with_token(&self, token: &str) -> Result<LoginResult, JsValue> {
		let mut credentials = HashMap::new();
		credentials.insert("token".to_string(), token.to_string());

		let response =
			self.auth_service.authenticate("token", credentials).map_err(|e| JsError::from_error(&e))?;

		self.handle_auth_response(response)
	}

	/// Also revokes the session token server-side, not just locally.
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
		self.flow_subsystem.shutdown();
	}
}

impl Default for WasmDB {
	fn default() -> Self {
		Self::new().expect("Failed to create WasmDB")
	}
}
