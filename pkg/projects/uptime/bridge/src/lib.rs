// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use napi_derive::napi;
use reifydb_node::ReifydbNode;
use reifydb_uptime::schema;

#[napi]
pub fn create(seed: u32) -> napi::Result<ReifydbNode> {
	ReifydbNode::new(seed, schema::migrations()).map_err(|e| napi::Error::from_reason(format!("{e:?}")))
}
