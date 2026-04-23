// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::shape::{RowShape, fingerprint::RowShapeFingerprint};

use super::MaterializedCatalog;

impl MaterializedCatalog {
	/// Insert a shape into the cache.
	///
	/// This does NOT persist the shape - it assumes it already exists in storage.
	pub fn set_row_shape(&self, shape: RowShape) {
		self.0.row_shapes.insert(shape.fingerprint(), shape);
	}

	/// Look up a shape by fingerprint (cache only).
	pub fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape> {
		self.0.row_shapes.get(&fingerprint).map(|entry| entry.value().clone())
	}

	/// List all cached shapes.
	pub fn list_row_shapes(&self) -> Vec<RowShape> {
		self.0.row_shapes.iter().map(|entry| entry.value().clone()).collect()
	}

	/// Get the number of cached shapes.
	pub fn row_shape_count(&self) -> usize {
		self.0.row_shapes.len()
	}
}
