// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
};

use reifydb_codec::key::sort::SortOrder;
use reifydb_value::fragment::Fragment;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
	Asc,
	Desc,
}

impl From<SortDirection> for SortOrder {
	fn from(direction: SortDirection) -> Self {
		match direction {
			SortDirection::Asc => SortOrder::Asc,
			SortDirection::Desc => SortOrder::Desc,
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
	pub column: Fragment,
	pub direction: SortDirection,
}

impl Display for SortDirection {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			SortDirection::Asc => write!(f, "ASC"),
			SortDirection::Desc => write!(f, "DESC"),
		}
	}
}

impl Display for SortKey {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{} {}", self.column.fragment(), self.direction)
	}
}
