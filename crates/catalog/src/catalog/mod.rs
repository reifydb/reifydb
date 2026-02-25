// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog wrapper that provides three-tier lookup methods.
//!
//! This struct wraps `MaterializedCatalog` and provides methods for looking up
//! catalog entities (tables, namespaces, views, etc.) using the three-tier lookup pattern:
//! 1. Check transactional changes first
//! 2. Check if deleted in transaction
//! 3. Check MaterializedCatalog at transaction version
//! 4. Fall back to storage as defensive measure

pub mod column;
pub mod dictionary;
pub mod flow;
pub mod flow_edge;
pub mod flow_node;
pub mod handler;
pub mod migration;
pub mod namespace;
pub mod policy;
pub mod primary_key;
pub mod primitive;
pub mod procedure;
pub mod resolve;
pub mod ringbuffer;
pub mod sequence;
pub mod series;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod user;
pub mod view;
pub mod vtable;

use crate::{materialized::MaterializedCatalog, schema::SchemaRegistry};

#[derive(Debug, Clone)]
pub struct Catalog {
	pub materialized: MaterializedCatalog,
	pub schema: SchemaRegistry,
}

impl Catalog {
	pub fn new(materialized: MaterializedCatalog, schema: SchemaRegistry) -> Self {
		Self {
			materialized,
			schema,
		}
	}

	pub fn testing() -> Self {
		Self::new(MaterializedCatalog::default(), SchemaRegistry::testing())
	}
}
