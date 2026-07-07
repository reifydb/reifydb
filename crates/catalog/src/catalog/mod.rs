// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Per-object-type catalog operations, one sibling module per object kind, each in the same shape:
//! admin-only mutations, transactional reads, name-and-id resolution. Resolution lives here rather
//! than in callers so DDL, DML and admin paths all resolve names through one set of helpers.

pub mod authentication;
pub mod binding;
pub mod column;
pub mod column_snapshot;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod flow_edge;
pub mod handler;
pub mod identity;
pub mod identity_attribute;
pub mod migration;
pub mod namespace;
pub mod object;
pub mod operator;
pub mod operator_settings;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod queue;
pub mod relationship;
pub mod resolve;
pub mod ringbuffer;
pub mod row_settings;
pub mod row_shape;
pub mod sequence;
pub mod series;
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
use reifydb_value::value::Value;

impl GetConfig for Catalog {
	fn get_config(&self, key: ConfigKey) -> Value {
		self.cache.get_config(key)
	}

	fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value {
		self.cache.get_config_at(key, version)
	}
}
