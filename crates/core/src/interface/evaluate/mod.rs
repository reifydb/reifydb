// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Expression-evaluation contract types shared between the engine, the planner, and the catalog.
//!
//! `TargetColumn` is the central value: a column-targeting expression (insert, update, default-value resolution) either
//! knows the fully-resolved column it is writing to (`TargetColumn::Resolved`) or describes the partial information
//! available before resolution (`TargetColumn::Partial`). Number-out-of-range descriptor construction lives here too so
//! range-violation diagnostics are assembled identically by every evaluator.

use reifydb_type::{error::NumberOutOfRangeDescriptor, value::r#type::Type};

use crate::interface::{
	catalog::property::ColumnPropertyKind,
	resolved::{ResolvedColumn, resolved_column_to_number_descriptor},
};

#[derive(Debug, Clone)]
pub enum TargetColumn {
	Resolved(ResolvedColumn),

	Partial {
		source_name: Option<String>,
		column_name: Option<String>,
		column_type: Type,
		properties: Vec<ColumnPropertyKind>,
	},
}

impl TargetColumn {
	pub fn column_type(&self) -> Type {
		match self {
			Self::Resolved(col) => col.column_type(),
			Self::Partial {
				column_type,
				..
			} => column_type.clone(),
		}
	}

	pub fn properties(&self) -> Vec<ColumnPropertyKind> {
		match self {
			Self::Resolved(col) => col.properties(),
			Self::Partial {
				properties,
				..
			} => properties.clone(),
		}
	}

	pub fn to_number_descriptor(&self) -> Option<NumberOutOfRangeDescriptor> {
		match self {
			Self::Resolved(col) => Some(resolved_column_to_number_descriptor(col)),
			Self::Partial {
				column_type,
				source_name,
				column_name,
				..
			} => {
				if source_name.is_some() || column_name.is_some() {
					Some(NumberOutOfRangeDescriptor {
						namespace: None,
						table: source_name.clone(),
						column: column_name.clone(),
						column_type: Some(column_type.clone()),
					})
				} else {
					None
				}
			}
		}
	}
}
