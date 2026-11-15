// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use napi::Error as NapiError;
use napi_derive::napi;
use reifydb_node::ReifydbNode;
use reifydb_uptime::migration_path;

#[napi]
pub fn create() -> Result<ReifydbNode, NapiError> {
	ReifydbNode::new(migration_path()).map_err(|e| NapiError::from_reason(format!("{e:?}")))
}
