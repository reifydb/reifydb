// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{ColumnId, ColumnPropertyId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnProperty {
	pub id: ColumnPropertyId,
	pub column: ColumnId,
	pub property: ColumnPropertyKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnPropertyKind {
	Saturation(ColumnSaturationPolicy),
}

impl ColumnPropertyKind {
	pub fn to_u8(&self) -> (u8, u8) {
		match self {
			ColumnPropertyKind::Saturation(policy) => match policy {
				ColumnSaturationPolicy::Error => (0x01, 0x01),
				ColumnSaturationPolicy::None => (0x01, 0x02),
			},
		}
	}

	pub fn from_u8(policy: u8, value: u8) -> ColumnPropertyKind {
		match (policy, value) {
			(0x01, 0x01) => ColumnPropertyKind::Saturation(ColumnSaturationPolicy::Error),
			(0x01, 0x02) => ColumnPropertyKind::Saturation(ColumnSaturationPolicy::None),
			_ => unimplemented!(),
		}
	}

	pub fn default_saturation_policy() -> Self {
		Self::Saturation(ColumnSaturationPolicy::default())
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnSaturationPolicy {
	Error,
	// Saturate,
	// Wrap,
	// Zero,
	None,
}

impl Display for ColumnPropertyKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			ColumnPropertyKind::Saturation(_) => f.write_str("saturation"),
		}
	}
}

pub const DEFAULT_COLUMN_SATURATION_POLICY: ColumnSaturationPolicy = ColumnSaturationPolicy::Error;

impl Default for ColumnSaturationPolicy {
	fn default() -> Self {
		Self::Error
	}
}
