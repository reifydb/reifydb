// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::fragment::Fragment;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier {
	pub object: ColumnObject,
	pub name: Fragment,
}

impl ColumnIdentifier {
	pub fn with_object(namespace: Fragment, object: Fragment, name: Fragment) -> Self {
		Self {
			object: ColumnObject::Qualified {
				namespace,
				name: object,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment, name: Fragment) -> Self {
		Self {
			object: ColumnObject::Alias(alias),
			name,
		}
	}

	pub fn into_owned(self) -> ColumnIdentifier {
		ColumnIdentifier {
			object: self.object,
			name: self.name,
		}
	}

	pub fn to_static(&self) -> ColumnIdentifier {
		ColumnIdentifier {
			object: self.object.clone(),
			name: Fragment::internal(self.name.text()),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnObject {
	Qualified {
		namespace: Fragment,
		name: Fragment,
	},

	Alias(Fragment),
}

impl ColumnObject {
	pub fn into_owned(self) -> ColumnObject {
		match self {
			ColumnObject::Qualified {
				namespace,
				name,
			} => ColumnObject::Qualified {
				namespace,
				name,
			},
			ColumnObject::Alias(alias) => ColumnObject::Alias(alias),
		}
	}

	pub fn to_static(&self) -> ColumnObject {
		match self {
			ColumnObject::Qualified {
				namespace,
				name,
			} => ColumnObject::Qualified {
				namespace: Fragment::internal(namespace.text()),
				name: Fragment::internal(name.text()),
			},
			ColumnObject::Alias(alias) => ColumnObject::Alias(Fragment::internal(alias.text())),
		}
	}

	pub fn as_fragment(&self) -> &Fragment {
		match self {
			ColumnObject::Qualified {
				name,
				..
			} => name,
			ColumnObject::Alias(alias) => alias,
		}
	}
}
