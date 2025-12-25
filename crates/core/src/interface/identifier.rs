// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Fragment;
use serde::{Deserialize, Serialize};

// NOTE: ColumnIdentifier is kept temporarily for backward compatibility with the expression system.
// It should be replaced with proper resolved types in the future.

/// Column identifier with primitive qualification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier {
	pub primitive: ColumnPrimitive,
	pub name: Fragment,
}

impl ColumnIdentifier {
	pub fn with_primitive(namespace: Fragment, primitive: Fragment, name: Fragment) -> Self {
		Self {
			primitive: ColumnPrimitive::Primitive {
				namespace,
				primitive,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment, name: Fragment) -> Self {
		Self {
			primitive: ColumnPrimitive::Alias(alias),
			name,
		}
	}

	pub fn into_owned(self) -> ColumnIdentifier {
		ColumnIdentifier {
			primitive: self.primitive,
			name: self.name,
		}
	}

	pub fn to_static(&self) -> ColumnIdentifier {
		ColumnIdentifier {
			primitive: self.primitive.clone(),
			name: Fragment::internal(self.name.text()),
		}
	}
}

/// How a column is qualified in plans (always fully qualified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnPrimitive {
	/// Fully qualified by namespace.primitive
	Primitive {
		namespace: Fragment,
		primitive: Fragment,
	},
	/// Qualified by alias (which maps to a fully qualified primitive)
	Alias(Fragment),
}

impl ColumnPrimitive {
	pub fn into_owned(self) -> ColumnPrimitive {
		match self {
			ColumnPrimitive::Primitive {
				namespace,
				primitive,
			} => ColumnPrimitive::Primitive {
				namespace,
				primitive,
			},
			ColumnPrimitive::Alias(alias) => ColumnPrimitive::Alias(alias),
		}
	}

	pub fn to_static(&self) -> ColumnPrimitive {
		match self {
			ColumnPrimitive::Primitive {
				namespace,
				primitive,
			} => ColumnPrimitive::Primitive {
				namespace: Fragment::internal(namespace.text()),
				primitive: Fragment::internal(primitive.text()),
			},
			ColumnPrimitive::Alias(alias) => ColumnPrimitive::Alias(Fragment::internal(alias.text())),
		}
	}

	pub fn as_fragment(&self) -> &Fragment {
		match self {
			ColumnPrimitive::Primitive {
				primitive,
				..
			} => primitive,
			ColumnPrimitive::Alias(alias) => alias,
		}
	}
}
