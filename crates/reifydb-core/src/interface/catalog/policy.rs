// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{ColumnPolicyId, TableColumnId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnPolicy {
	pub id: ColumnPolicyId,
	pub column: TableColumnId,
	pub policy: ColumnPolicyKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnPolicyKind {
	Saturation(ColumnSaturationPolicy),
}

impl ColumnPolicyKind {
	pub fn to_u8(&self) -> (u8, u8) {
		match self {
			ColumnPolicyKind::Saturation(policy) => match policy {
				ColumnSaturationPolicy::Error => (0x01, 0x01),
				ColumnSaturationPolicy::Undefined => {
					(0x01, 0x02)
				}
			},
		}
	}

	pub fn from_u8(policy: u8, value: u8) -> ColumnPolicyKind {
		match (policy, value) {
			(0x01, 0x01) => ColumnPolicyKind::Saturation(
				ColumnSaturationPolicy::Error,
			),
			(0x01, 0x02) => ColumnPolicyKind::Saturation(
				ColumnSaturationPolicy::Undefined,
			),
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
	Undefined,
}

impl Display for ColumnPolicyKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			ColumnPolicyKind::Saturation(_) => {
				f.write_str("saturation")
			}
		}
	}
}

pub const DEFAULT_COLUMN_SATURATION_POLICY: ColumnSaturationPolicy =
	ColumnSaturationPolicy::Error;

impl Default for ColumnSaturationPolicy {
	fn default() -> Self {
		Self::Error
	}
}
