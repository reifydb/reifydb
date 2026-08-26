// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use napi_derive::napi;
use reifydb::{
	Database, Frame, IdentityId, MigrationSource, Value, auth::service::AuthResponse, embedded,
	runtime::RuntimeConfig,
};
use reifydb_value::value::uuid::Uuid7;

#[napi]
pub struct ReifydbNode {
	db: Database,
}

impl ReifydbNode {
	pub fn new(seed: u32, migrations: impl Into<MigrationSource>) -> reifydb::Result<Self> {
		let db = embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(seed as u64))
			.with_migrations(migrations)
			.build()?;
		Ok(Self {
			db,
		})
	}
}

#[napi]
impl ReifydbNode {
	#[napi]
	pub fn command_root(&self, rql: String) -> napi::Result<String> {
		let frames = self.db.command_as_root(&rql, ()).map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
		Ok(frames_to_json(&frames))
	}

	#[napi]
	pub fn query_root(&self, rql: String) -> napi::Result<String> {
		let frames = self.db.query_as_root(&rql, ()).map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
		Ok(frames_to_json(&frames))
	}

	#[napi]
	pub fn command_as(&self, identity: String, rql: String) -> napi::Result<String> {
		let identity = parse_identity(&identity)?;
		let frames =
			self.db.command_as(identity, &rql, ()).map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
		Ok(frames_to_json(&frames))
	}

	#[napi]
	pub fn query_as(&self, identity: String, rql: String) -> napi::Result<String> {
		let identity = parse_identity(&identity)?;
		let frames =
			self.db.query_as(identity, &rql, ()).map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
		Ok(frames_to_json(&frames))
	}

	#[napi]
	pub fn authenticate(&self, method: String, credentials: HashMap<String, String>) -> napi::Result<String> {
		let response = self
			.db
			.auth_service()
			.authenticate(&method, credentials)
			.map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
		Ok(auth_response_to_json(&response))
	}
}

fn parse_identity(raw: &str) -> napi::Result<IdentityId> {
	let uuid = uuid::Uuid::parse_str(raw).map_err(|e| napi::Error::from_reason(format!("invalid identity: {e}")))?;
	Ok(IdentityId::new(Uuid7::from(uuid)))
}

fn frames_to_json(frames: &[Frame]) -> String {
	let rows: Vec<serde_json::Value> = frames
		.iter()
		.flat_map(|frame| frame.to_rows())
		.map(|row| {
			let map: serde_json::Map<String, serde_json::Value> =
				row.into_iter().map(|(name, value): (String, Value)| (name, value.to_string().into())).collect();
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
