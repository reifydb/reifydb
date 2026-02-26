// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

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
