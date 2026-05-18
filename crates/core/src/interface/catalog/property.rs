// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
};

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
	Saturation(ColumnSaturationStrategy),
}

impl ColumnPropertyKind {
	pub fn to_u8(&self) -> (u8, u8) {
		match self {
			ColumnPropertyKind::Saturation(strategy) => match strategy {
				ColumnSaturationStrategy::Error => (0x01, 0x01),
				ColumnSaturationStrategy::None => (0x01, 0x02),
			},
		}
	}

	pub fn from_u8(strategy: u8, value: u8) -> ColumnPropertyKind {
		match (strategy, value) {
			(0x01, 0x01) => ColumnPropertyKind::Saturation(ColumnSaturationStrategy::Error),
			(0x01, 0x02) => ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None),
			_ => unimplemented!(),
		}
	}

	pub fn default_saturation_strategy() -> Self {
		Self::Saturation(ColumnSaturationStrategy::default())
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum ColumnSaturationStrategy {
	#[default]
	Error,

	None,
}

impl Display for ColumnPropertyKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ColumnPropertyKind::Saturation(_) => f.write_str("saturation"),
		}
	}
}

pub const DEFAULT_COLUMN_SATURATION_STRATEGY: ColumnSaturationStrategy = ColumnSaturationStrategy::Error;
