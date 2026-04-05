// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod actor;
pub mod config;
pub(crate) mod scanner;
pub(crate) mod stats;

use reifydb_core::{interface::catalog::shape::ShapeId, row::RowTtl};

/// Trait for providing TTL configurations to the actor.
pub trait ListRowTtls: Clone + Send + Sync + 'static {
	fn list_row_ttls(&self) -> Vec<(ShapeId, RowTtl)>;
}
