// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod config;
pub mod hot;
pub(crate) mod scanner;
pub(crate) mod stats;

use reifydb_core::{interface::catalog::shape::ShapeId, row::RowTtl};

/// Trait for providing TTL configurations to the GC actor.
///
/// Implemented by the engine layer wrapping `MaterializedCatalog`.
/// This keeps `store-multi` independent of `reifydb-catalog`.
pub trait RowTtlProvider: Clone + Send + Sync + 'static {
	/// Returns all shapes that currently have TTL configurations.
	/// Called once per scan cycle.
	fn row_ttls(&self) -> Vec<(ShapeId, RowTtl)>;
}
