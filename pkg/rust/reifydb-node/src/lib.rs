// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use napi_derive::napi;
#[cfg(reifydb_dst)]
use reifydb::runtime::RuntimeConfig;
use reifydb::{
	Database, Frame, IdentityId, Migration, MigrationSource, Value, auth::service::AuthResponse, embedded,
};
use reifydb_value::value::uuid::Uuid7;
use tokio::task::spawn_blocking;

#[napi]
pub struct ReifydbNode {
	db: Arc<Database>,
}

#[cfg(reifydb_dst)]
impl ReifydbNode {
	pub fn new(seed: u32, migrations: impl Into<MigrationSource>) -> reifydb::Result<Self> {
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
pub fn open_with_migrations(entries: Vec<MigrationEntry>) -> napi::Result<ReifydbNode> {
	let mut builder = embedded::memory();
	if !entries.is_empty() {
		let sources = entries.into_iter().map(migration_source).collect();
		builder = builder.with_migrations(MigrationSource::Multiple(sources));
	}
	let db = builder.build().map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
	Ok(ReifydbNode {
		db: Arc::new(db),
	})
}

#[napi]
impl ReifydbNode {
	#[napi(js_name = "command_root")]
	pub async fn command_root(&self, rql: String) -> napi::Result<String> {
		let db = self.db.clone();
		spawn_blocking(move || db.command_as_root(&rql, ()))
			.await
			.expect("blocking task panicked")
			.map(|frames| frames_to_json(&frames))
			.map_err(|e| napi::Error::from_reason(format!("{e:?}")))
	}

	#[napi(js_name = "query_root")]
	pub async fn query_root(&self, rql: String) -> napi::Result<String> {
		let db = self.db.clone();
		spawn_blocking(move || db.query_as_root(&rql, ()))
			.await
			.expect("blocking task panicked")
			.map(|frames| frames_to_json(&frames))
			.map_err(|e| napi::Error::from_reason(format!("{e:?}")))
	}

	#[napi(js_name = "command_as")]
	pub async fn command_as(&self, identity: String, rql: String) -> napi::Result<String> {
		let db = self.db.clone();
		spawn_blocking(move || {
			let identity = parse_identity(&identity)?;
			db.command_as(identity, &rql, ()).map_err(|e| napi::Error::from_reason(format!("{e:?}")))
		})
		.await
		.expect("blocking task panicked")
		.map(|frames| frames_to_json(&frames))
	}

	#[napi(js_name = "query_as")]
	pub async fn query_as(&self, identity: String, rql: String) -> napi::Result<String> {
		let db = self.db.clone();
		spawn_blocking(move || {
			let identity = parse_identity(&identity)?;
			db.query_as(identity, &rql, ()).map_err(|e| napi::Error::from_reason(format!("{e:?}")))
		})
		.await
		.expect("blocking task panicked")
		.map(|frames| frames_to_json(&frames))
	}

	#[napi]
	pub async fn authenticate(&self, method: String, credentials: HashMap<String, String>) -> napi::Result<String> {
		let db = self.db.clone();
		spawn_blocking(move || db.auth_service().authenticate(&method, credentials))
			.await
			.expect("blocking task panicked")
			.map(|response| auth_response_to_json(&response))
			.map_err(|e| napi::Error::from_reason(format!("{e:?}")))
	}
}

fn parse_identity(raw: &str) -> napi::Result<IdentityId> {
	let uuid =
		uuid::Uuid::parse_str(raw).map_err(|e| napi::Error::from_reason(format!("invalid identity: {e}")))?;
	Ok(IdentityId::new(Uuid7::from(uuid)))
}

fn frames_to_json(frames: &[Frame]) -> String {
	let rows: Vec<serde_json::Value> = frames
		.iter()
		.flat_map(|frame| frame.to_rows())
		.map(|row| {
			let map: serde_json::Map<String, serde_json::Value> = row
				.into_iter()
				.map(|(name, value): (String, Value)| (name, value.to_string().into()))
				.collect();
			serde_json::Value::Object(map)
		})
		.collect();
	serde_json::to_string(&rows).expect("row values are all plain strings")
}

fn auth_response_to_json(response: &AuthResponse) -> String {
	let json = match response {
		AuthResponse::Authenticated {
			identity,
			token,
		} => serde_json::json!({"status": "authenticated", "identity": identity.to_string(), "token": token}),
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => serde_json::json!({"status": "challenge", "challengeId": challenge_id, "payload": payload}),
		AuthResponse::Failed {
			reason,
		} => serde_json::json!({"status": "failed", "reason": reason}),
	};
	serde_json::to_string(&json).expect("auth response fields are all plain strings")
}
