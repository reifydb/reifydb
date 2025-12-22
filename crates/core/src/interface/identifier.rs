// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Fragment;
use serde::{Deserialize, Serialize};

// NOTE: ColumnIdentifier is kept temporarily for backward compatibility with the expression system.
// It should be replaced with proper resolved types in the future.

/// Column identifier with source qualification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier {
	pub source: ColumnSource,
	pub name: Fragment,
}

impl ColumnIdentifier {
	pub fn with_source(namespace: Fragment, source: Fragment, name: Fragment) -> Self {
		Self {
			source: ColumnSource::Source {
				namespace,
				source,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment, name: Fragment) -> Self {
		Self {
			source: ColumnSource::Alias(alias),
			name,
		}
	}

	pub fn into_owned(self) -> ColumnIdentifier {
		ColumnIdentifier {
			source: self.source,
			name: self.name,
		}
	}

	pub fn to_static(&self) -> ColumnIdentifier {
		ColumnIdentifier {
			source: self.source.clone(),
			name: Fragment::internal(self.name.text()),
		}
	}
}

/// How a column is qualified in plans (always fully qualified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnSource {
	/// Fully qualified by namespace.source
	Source {
		namespace: Fragment,
		source: Fragment,
	},
	/// Qualified by alias (which maps to a fully qualified source)
	Alias(Fragment),
}

impl ColumnSource {
	pub fn into_owned(self) -> ColumnSource {
		match self {
			ColumnSource::Source {
				namespace,
				source,
			} => ColumnSource::Source {
				namespace,
				source,
			},
			ColumnSource::Alias(alias) => ColumnSource::Alias(alias),
		}
	}

	pub fn to_static(&self) -> ColumnSource {
		match self {
			ColumnSource::Source {
				namespace,
				source,
			} => ColumnSource::Source {
				namespace: Fragment::internal(namespace.text()),
				source: Fragment::internal(source.text()),
			},
			ColumnSource::Alias(alias) => ColumnSource::Alias(Fragment::internal(alias.text())),
		}
	}

	pub fn as_fragment(&self) -> &Fragment {
		match self {
			ColumnSource::Source {
				source,
				..
			} => source,
			ColumnSource::Alias(alias) => alias,
		}
	}
}
