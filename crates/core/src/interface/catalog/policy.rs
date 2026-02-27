// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{ColumnId, ColumnPolicyId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnPolicy {
	pub id: ColumnPolicyId,
	pub column: ColumnId,
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
				ColumnSaturationPolicy::None => (0x01, 0x02),
			},
		}
	}

	pub fn from_u8(policy: u8, value: u8) -> ColumnPolicyKind {
		match (policy, value) {
			(0x01, 0x01) => ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Error),
			(0x01, 0x02) => ColumnPolicyKind::Saturation(ColumnSaturationPolicy::None),
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

impl Display for ColumnPolicyKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			ColumnPolicyKind::Saturation(_) => f.write_str("saturation"),
		}
	}
}

pub const DEFAULT_COLUMN_SATURATION_POLICY: ColumnSaturationPolicy = ColumnSaturationPolicy::Error;

impl Default for ColumnSaturationPolicy {
	fn default() -> Self {
		Self::Error
	}
}

// Security policy types (merged from security_policy.rs)

pub type SecurityPolicyId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PolicyTargetType {
	Table,
	Column,
	Namespace,
	Procedure,
	Function,
	Flow,
	Subscription,
	Series,
	Dictionary,
	Session,
	Feature,
}

impl PolicyTargetType {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Table => "table",
			Self::Column => "column",
			Self::Namespace => "namespace",
			Self::Procedure => "procedure",
			Self::Function => "function",
			Self::Flow => "flow",
			Self::Subscription => "subscription",
			Self::Series => "series",
			Self::Dictionary => "dictionary",
			Self::Session => "session",
			Self::Feature => "feature",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SecurityPolicyDef {
	pub id: SecurityPolicyId,
	pub name: Option<String>,
	pub target_type: PolicyTargetType,
	pub target_namespace: Option<String>,
	pub target_object: Option<String>,
	pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SecurityPolicyOperationDef {
	pub policy_id: SecurityPolicyId,
	pub operation: String,
	pub body_source: String,
}

pub struct SecurityPolicyToCreate {
	pub name: Option<String>,
	pub target_type: PolicyTargetType,
	pub target_namespace: Option<String>,
	pub target_object: Option<String>,
	pub operations: Vec<SecurityPolicyOpToCreate>,
}

pub struct SecurityPolicyOpToCreate {
	pub operation: String,
	pub body_source: String,
}
