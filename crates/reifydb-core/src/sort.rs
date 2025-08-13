// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	fmt,
	fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

use crate::OwnedSpan;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
	Asc,
	Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
	pub column: OwnedSpan,
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
		write!(f, "{} {}", self.column.fragment, self.direction)
	}
}
