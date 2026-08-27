// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use napi::{Error as NapiError, Result, bindgen_prelude::Either};
use napi_derive::napi;
#[cfg(reifydb_dst)]
use reifydb::runtime::RuntimeConfig;
use reifydb::{
	Database, Frame as CoreFrame, IdentityId, Migration, MigrationSource, Result as ReifyResult,
	auth::service::AuthResponse, embedded,
};
use reifydb_codec::json::to::convert_frames;
use reifydb_sub_server::wire::{WireParams, WireValue};
use reifydb_value::{params::Params, value::uuid::Uuid7};
use serde_json::{Value as JsonValue, json, to_string as json_to_string, to_value};
use tokio::task::spawn_blocking;
use uuid::Uuid;

#[napi]
pub struct ReifydbNode {
	db: Arc<Database>,
}

#[napi(object)]
pub struct ParamValue {
	pub r#type: String,
	pub value: String,
}

#[napi(object)]
pub struct Column {
	pub name: String,
	pub r#type: JsonValue,
	pub payload: Vec<String>,
}

#[napi(object)]
pub struct Frame {
	pub columns: Vec<Column>,
}

fn frames_to_napi(frames: &[CoreFrame]) -> Vec<Frame> {
	convert_frames(frames)
		.into_iter()
		.map(|frame| Frame {
			columns: frame
				.columns
				.into_iter()
				.map(|column| Column {
					name: column.name,
					r#type: to_value(&column.r#type)
						.expect("value type is always representable as JSON"),
					payload: column.payload,
				})
				.collect(),
		})
		.collect()
}

type ParamsInput = Either<Vec<ParamValue>, HashMap<String, ParamValue>>;

fn parse_params(params: Option<ParamsInput>) -> Result<Params> {
	let wire = match params {
		None => return Ok(Params::None),
		Some(Either::A(items)) => WireParams::Positional(items.into_iter().map(to_wire_value).collect()),
		Some(Either::B(map)) => {
			WireParams::Named(map.into_iter().map(|(name, value)| (name, to_wire_value(value))).collect())
		}
	};
	wire.into_params().map_err(NapiError::from_reason)
}

fn to_wire_value(param: ParamValue) -> WireValue {
	WireValue {
		type_name: param.r#type,
		value: param.value,
	}
}

#[cfg(reifydb_dst)]
impl ReifydbNode {
	pub fn new(seed: u32, migrations: impl Into<MigrationSource>) -> ReifyResult<Self> {
		let db = embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(seed as u64))
			.with_migrations(migrations)
			.build()?;
		Ok(Self {
			db: Arc::new(db),
		})
	}
}

#[cfg(not(reifydb_dst))]
impl ReifydbNode {
	pub fn new(_seed: u32, migrations: impl Into<MigrationSource>) -> ReifyResult<Self> {
		let db = embedded::memory().with_migrations(migrations).build()?;
		Ok(Self {
			db: Arc::new(db),
		})
	}
}

#[cfg(not(reifydb_dst))]
#[napi(object)]
pub struct MigrationEntry {
	pub dir: Option<String>,
	pub name: Option<String>,
	pub statements: Option<Vec<String>>,
	pub rollback: Option<Vec<String>>,
}

#[cfg(not(reifydb_dst))]
fn migration_source(entry: MigrationEntry) -> MigrationSource {
	match entry.dir {
		Some(dir) => MigrationSource::Directory(PathBuf::from(dir)),
		None => {
			let name = entry.name.unwrap_or_default();
			let statements = entry.statements.unwrap_or_default();
			let migration = match entry.rollback {
				Some(rollback) => Migration::with_rollback(name, statements, rollback),
				None => Migration::new(name, statements),
			};
			MigrationSource::List(vec![migration])
		}
	}
}

#[cfg(not(reifydb_dst))]
#[napi(js_name = "open_with_migrations")]
pub fn open_with_migrations(entries: Vec<MigrationEntry>) -> Result<ReifydbNode> {
	let mut builder = embedded::memory();
	if !entries.is_empty() {
		let sources = entries.into_iter().map(migration_source).collect();
		builder = builder.with_migrations(MigrationSource::Multiple(sources));
	}
	let db = builder.build().map_err(|e| NapiError::from_reason(format!("{e:?}")))?;
	Ok(ReifydbNode {
		db: Arc::new(db),
	})
}

#[napi]
impl ReifydbNode {
	#[napi(js_name = "admin_root")]
	pub async fn admin_root(
		&self,
		rql: String,
		params: Option<Either<Vec<ParamValue>, HashMap<String, ParamValue>>>,
	) -> Result<Vec<Frame>> {
		let db = self.db.clone();
		let params = parse_params(params)?;
		spawn_blocking(move || db.admin_as_root(&rql, params))
			.await
			.expect("blocking task panicked")
			.map(|frames| frames_to_napi(&frames))
			.map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}

	#[napi(js_name = "command_root")]
	pub async fn command_root(
		&self,
		rql: String,
		params: Option<Either<Vec<ParamValue>, HashMap<String, ParamValue>>>,
	) -> Result<Vec<Frame>> {
		let db = self.db.clone();
		let params = parse_params(params)?;
		spawn_blocking(move || db.command_as_root(&rql, params))
			.await
			.expect("blocking task panicked")
			.map(|frames| frames_to_napi(&frames))
			.map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}

	#[napi(js_name = "query_root")]
	pub async fn query_root(
		&self,
		rql: String,
		params: Option<Either<Vec<ParamValue>, HashMap<String, ParamValue>>>,
	) -> Result<Vec<Frame>> {
		let db = self.db.clone();
		let params = parse_params(params)?;
		spawn_blocking(move || db.query_as_root(&rql, params))
			.await
			.expect("blocking task panicked")
			.map(|frames| frames_to_napi(&frames))
			.map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}

	#[napi(js_name = "admin_as")]
	pub async fn admin_as(
		&self,
		identity: String,
		rql: String,
		params: Option<Either<Vec<ParamValue>, HashMap<String, ParamValue>>>,
	) -> Result<Vec<Frame>> {
		let db = self.db.clone();
		let params = parse_params(params)?;
		spawn_blocking(move || {
			let identity = parse_identity(&identity)?;
			db.admin_as(identity, &rql, params).map_err(|e| NapiError::from_reason(format!("{e:?}")))
		})
		.await
		.expect("blocking task panicked")
		.map(|frames| frames_to_napi(&frames))
	}

	#[napi(js_name = "command_as")]
	pub async fn command_as(
		&self,
		identity: String,
		rql: String,
		params: Option<Either<Vec<ParamValue>, HashMap<String, ParamValue>>>,
	) -> Result<Vec<Frame>> {
		let db = self.db.clone();
		let params = parse_params(params)?;
		spawn_blocking(move || {
			let identity = parse_identity(&identity)?;
			db.command_as(identity, &rql, params).map_err(|e| NapiError::from_reason(format!("{e:?}")))
		})
		.await
		.expect("blocking task panicked")
		.map(|frames| frames_to_napi(&frames))
	}

	#[napi(js_name = "query_as")]
	pub async fn query_as(
		&self,
		identity: String,
		rql: String,
		params: Option<Either<Vec<ParamValue>, HashMap<String, ParamValue>>>,
	) -> Result<Vec<Frame>> {
		let db = self.db.clone();
		let params = parse_params(params)?;
		spawn_blocking(move || {
			let identity = parse_identity(&identity)?;
			db.query_as(identity, &rql, params).map_err(|e| NapiError::from_reason(format!("{e:?}")))
		})
		.await
		.expect("blocking task panicked")
		.map(|frames| frames_to_napi(&frames))
	}

	#[napi]
	pub async fn authenticate(&self, method: String, credentials: HashMap<String, String>) -> Result<String> {
		let db = self.db.clone();
		spawn_blocking(move || db.auth_service().authenticate(&method, credentials))
			.await
			.expect("blocking task panicked")
			.map(|response| auth_response_to_json(&response))
			.map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}
}

fn parse_identity(raw: &str) -> Result<IdentityId> {
	let uuid = Uuid::parse_str(raw).map_err(|e| NapiError::from_reason(format!("invalid identity: {e}")))?;
	Ok(IdentityId::new(Uuid7::from(uuid)))
}

fn auth_response_to_json(response: &AuthResponse) -> String {
	let json = match response {
		AuthResponse::Authenticated {
			identity,
			token,
		} => json!({"status": "authenticated", "identity": identity.to_string(), "token": token}),
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => json!({"status": "challenge", "challengeId": challenge_id, "payload": payload}),
		AuthResponse::Failed {
			reason,
		} => json!({"status": "failed", "reason": reason}),
	};
	json_to_string(&json).expect("auth response fields are all plain strings")
}
