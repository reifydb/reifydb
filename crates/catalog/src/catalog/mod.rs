// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Catalog wrapper that provides three-tier lookup methods.
//!
//! This struct wraps `MaterializedCatalog` and provides methods for looking up
//! catalog entities (tables, namespaces, views, etc.) using the three-tier lookup pattern:
//! 1. Check transactional changes first
//! 2. Check if deleted in transaction
//! 3. Check MaterializedCatalog at transaction version
//! 4. Fall back to storage as defensive measure

pub mod authentication;
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
	interface::catalog::config::{GetSystemConfig, SystemConfigKey},
};
use reifydb_type::value::Value;

impl GetSystemConfig for Catalog {
	fn get_system_config(&self, key: SystemConfigKey) -> Value {
		self.materialized.get_system_config(key)
	}

	fn get_system_config_at(&self, key: SystemConfigKey, version: CommitVersion) -> Value {
		self.materialized.get_system_config_at(key, version)
	}
}
