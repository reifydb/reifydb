// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Fragment;
use serde::{Deserialize, Serialize};

// NOTE: ColumnIdentifier is kept temporarily for backward compatibility with the expression system.
// It should be replaced with proper resolved types in the future.

/// Column identifier with source qualification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnIdentifier<'a> {
	pub source: ColumnSource<'a>,
	pub name: Fragment<'a>,
}

impl<'a> ColumnIdentifier<'a> {
	pub fn with_source(namespace: Fragment<'a>, source: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			source: ColumnSource::Source {
				namespace,
				source,
			},
			name,
		}
	}

	pub fn with_alias(alias: Fragment<'a>, name: Fragment<'a>) -> Self {
		Self {
			source: ColumnSource::Alias(alias),
			name,
		}
	}

	pub fn into_owned(self) -> ColumnIdentifier<'static> {
		ColumnIdentifier {
			source: self.source.into_owned(),
			name: Fragment::Owned(self.name.into_owned()),
		}
	}

	pub fn to_static(&self) -> ColumnIdentifier<'static> {
		ColumnIdentifier {
			source: self.source.to_static(),
			name: Fragment::owned_internal(self.name.text()),
		}
	}
}

/// How a column is qualified in plans (always fully qualified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnSource<'a> {
	/// Fully qualified by namespace.source
	Source {
		namespace: Fragment<'a>,
		source: Fragment<'a>,
	},
	/// Qualified by alias (which maps to a fully qualified source)
	Alias(Fragment<'a>),
}

impl<'a> ColumnSource<'a> {
	pub fn into_owned(self) -> ColumnSource<'static> {
		match self {
			ColumnSource::Source {
				namespace,
				source,
			} => ColumnSource::Source {
				namespace: Fragment::Owned(namespace.into_owned()),
				source: Fragment::Owned(source.into_owned()),
			},
			ColumnSource::Alias(alias) => ColumnSource::Alias(Fragment::Owned(alias.into_owned())),
		}
	}

	pub fn to_static(&self) -> ColumnSource<'static> {
		match self {
			ColumnSource::Source {
				namespace,
				source,
			} => ColumnSource::Source {
				namespace: Fragment::owned_internal(namespace.text()),
				source: Fragment::owned_internal(source.text()),
			},
			ColumnSource::Alias(alias) => ColumnSource::Alias(Fragment::owned_internal(alias.text())),
		}
	}

	pub fn as_fragment(&self) -> &Fragment<'a> {
		match self {
			ColumnSource::Source {
				source,
				..
			} => source,
			ColumnSource::Alias(alias) => alias,
		}
	}
}
