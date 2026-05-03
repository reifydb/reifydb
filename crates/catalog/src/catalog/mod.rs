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

use std::sync::Arc;

use reifydb_core::interface::catalog::vtable::VTable;

use crate::{Result, cache::CatalogCache};

#[derive(Debug, Clone)]
pub struct Catalog {
	pub(crate) cache: CatalogCache,
}

impl Catalog {
	pub fn new(cache: CatalogCache) -> Self {
		Self {
			cache,
		}
	}

	pub fn testing() -> Self {
		Self::new(CatalogCache::new())
	}

	pub fn cache(&self) -> &CatalogCache {
		&self.cache
	}

	pub fn register_vtable_user(&self, def: Arc<VTable>) -> Result<()> {
		self.cache.register_vtable_user(def)
	}
}

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
};
use reifydb_type::value::Value;

impl GetConfig for Catalog {
	fn get_config(&self, key: ConfigKey) -> Value {
		self.cache.get_config(key)
	}

	fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value {
		self.cache.get_config_at(key, version)
	}
}
