// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::fragment::Fragment;
use serde::{Deserialize, Serialize};

/// Column identifier with schema qualification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier {
	pub schema: ColumnSchema,
	pub name: Fragment,
}

impl ColumnIdentifier {
	pub fn with_schema(namespace: Fragment, schema: Fragment, name: Fragment) -> Self {
		Self {
			schema: ColumnSchema::Qualified {
				namespace,
				name: schema,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment, name: Fragment) -> Self {
		Self {
			schema: ColumnSchema::Alias(alias),
			name,
		}
	}

	pub fn into_owned(self) -> ColumnIdentifier {
		ColumnIdentifier {
			schema: self.schema,
			name: self.name,
		}
	}

	pub fn to_static(&self) -> ColumnIdentifier {
		ColumnIdentifier {
			schema: self.schema.clone(),
			name: Fragment::internal(self.name.text()),
		}
	}
}

/// How a column is qualified in plans (always fully qualified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnSchema {
	/// Fully qualified by namespace.schema
	Qualified {
		namespace: Fragment,
		name: Fragment,
	},
	/// Qualified by alias (which maps to a fully qualified schema)
	Alias(Fragment),
}

impl ColumnSchema {
	pub fn into_owned(self) -> ColumnSchema {
		match self {
			ColumnSchema::Qualified {
				namespace,
				name,
			} => ColumnSchema::Qualified {
				namespace,
				name,
			},
			ColumnSchema::Alias(alias) => ColumnSchema::Alias(alias),
		}
	}

	pub fn to_static(&self) -> ColumnSchema {
		match self {
			ColumnSchema::Qualified {
				namespace,
				name,
			} => ColumnSchema::Qualified {
				namespace: Fragment::internal(namespace.text()),
				name: Fragment::internal(name.text()),
			},
			ColumnSchema::Alias(alias) => ColumnSchema::Alias(Fragment::internal(alias.text())),
		}
	}

	pub fn as_fragment(&self) -> &Fragment {
		match self {
			ColumnSchema::Qualified {
				name,
				..
			} => name,
			ColumnSchema::Alias(alias) => alias,
		}
	}
}
