// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::fragment::Fragment;
use serde::{Deserialize, Serialize};

/// Column identifier with shape qualification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier {
	pub shape: ColumnShape,
	pub name: Fragment,
}

impl ColumnIdentifier {
	pub fn with_shape(namespace: Fragment, shape: Fragment, name: Fragment) -> Self {
		Self {
			shape: ColumnShape::Qualified {
				namespace,
				name: shape,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment, name: Fragment) -> Self {
		Self {
			shape: ColumnShape::Alias(alias),
			name,
		}
	}

	pub fn into_owned(self) -> ColumnIdentifier {
		ColumnIdentifier {
			shape: self.shape,
			name: self.name,
		}
	}

	pub fn to_static(&self) -> ColumnIdentifier {
		ColumnIdentifier {
			shape: self.shape.clone(),
			name: Fragment::internal(self.name.text()),
		}
	}
}

/// How a column is qualified in plans (always fully qualified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnShape {
	/// Fully qualified by namespace.shape
	Qualified {
		namespace: Fragment,
		name: Fragment,
	},
	/// Qualified by alias (which maps to a fully qualified shape)
	Alias(Fragment),
}

impl ColumnShape {
	pub fn into_owned(self) -> ColumnShape {
		match self {
			ColumnShape::Qualified {
				namespace,
				name,
			} => ColumnShape::Qualified {
				namespace,
				name,
			},
			ColumnShape::Alias(alias) => ColumnShape::Alias(alias),
		}
	}

	pub fn to_static(&self) -> ColumnShape {
		match self {
			ColumnShape::Qualified {
				namespace,
				name,
			} => ColumnShape::Qualified {
				namespace: Fragment::internal(namespace.text()),
				name: Fragment::internal(name.text()),
			},
			ColumnShape::Alias(alias) => ColumnShape::Alias(Fragment::internal(alias.text())),
		}
	}

	pub fn as_fragment(&self) -> &Fragment {
		match self {
			ColumnShape::Qualified {
				name,
				..
			} => name,
			ColumnShape::Alias(alias) => alias,
		}
	}
}
