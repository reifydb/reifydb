// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use napi_derive::napi;
use reifydb::{Frame, Value, embedded, runtime::RuntimeConfig, testing::db::TestDb};
use reifydb_uptime::schema;

#[napi]
pub struct DstEngine {
	db: TestDb,
}

#[napi]
impl DstEngine {
	#[napi(constructor)]
	pub fn new(seed: u32) -> napi::Result<Self> {
		let db = embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(seed as u64))
			.with_migrations(schema::migrations())
			.build()
			.map_err(|e| napi::Error::from_reason(format!("boot failed: {e:?}")))?;
		Ok(Self {
			db: TestDb::from(db),
		})
	}

	#[napi]
	pub fn command(&self, rql: String) -> napi::Result<String> {
		let frames = self.db.try_command(&rql).map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
		Ok(frames_to_json(&frames))
	}

	#[napi]
	pub fn query(&self, rql: String) -> napi::Result<String> {
		let frames = self.db.try_query(&rql).map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
		Ok(frames_to_json(&frames))
	}
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
