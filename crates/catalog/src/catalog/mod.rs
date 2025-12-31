// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Catalog wrapper that provides three-tier lookup methods.
//!
//! This struct wraps `MaterializedCatalog` and provides methods for looking up
//! catalog entities (tables, namespaces, views, etc.) using the three-tier lookup pattern:
//! 1. Check transactional changes first
//! 2. Check if deleted in transaction
//! 3. Check MaterializedCatalog at transaction version
//! 4. Fall back to storage as defensive measure

mod dictionary;
mod flow;
mod namespace;
mod resolve;
mod ringbuffer;
mod table;
mod view;
mod vtable;

use crate::MaterializedCatalog;

/// Catalog wrapper that owns a `MaterializedCatalog` and provides three-tier lookup methods.
///
/// The catalog is cheap to clone (Arc-based internally).
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
}

impl Default for Catalog {
	fn default() -> Self {
		Self::new(MaterializedCatalog::default())
	}
}

impl From<MaterializedCatalog> for Catalog {
	fn from(materialized: MaterializedCatalog) -> Self {
		Self::new(materialized)
	}
}
