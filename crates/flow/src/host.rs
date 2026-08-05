// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::encoded::shape::{RowShape, fingerprint::RowShapeFingerprint};

pub trait HostRowShape: Send + Sync {
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape>;
}

pub struct StandardHostRowShape {
	catalog: Catalog,
}

impl StandardHostRowShape {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			catalog,
		}
	}
}

impl HostRowShape for StandardHostRowShape {
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape> {
		self.catalog.cache().find_row_shape(fingerprint)
	}
}
