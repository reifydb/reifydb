// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod authentication;
pub mod binding;
pub mod column;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod flow_edge;
pub mod flow_node;
pub mod handler;
pub mod identity;
pub mod migration;
pub mod namespace;
pub mod operator_ttl;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod resolve;
pub mod ringbuffer;
pub mod row_shape;
pub mod row_ttl;
pub mod sequence;
pub mod series;
pub mod shape;
pub mod sink;
pub mod source;
pub mod sumtype;
pub mod table;
pub mod test;
pub mod view;
pub mod vtable;

use crate::materialized::MaterializedCatalog;

#[derive(Debug, Clone)]
pub struct Catalog {
	pub materialized: MaterializedCatalog,
}

impl Catalog {
	pub fn new(materialized: MaterializedCatalog) -> Self {
		Self {
			materialized,
		}
	}

	pub fn testing() -> Self {
		Self::new(MaterializedCatalog::new())
	}
}

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
};
use reifydb_type::value::Value;

impl GetConfig for Catalog {
	fn get_config(&self, key: ConfigKey) -> Value {
		self.materialized.get_config(key)
	}

	fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value {
		self.materialized.get_config_at(key, version)
	}
}
