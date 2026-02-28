// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

// Security policy types

pub type PolicyId = u64;

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
	View,
	RingBuffer,
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
			Self::View => "view",
			Self::RingBuffer => "ringbuffer",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyDef {
	pub id: PolicyId,
	pub name: Option<String>,
	pub target_type: PolicyTargetType,
	pub target_namespace: Option<String>,
	pub target_object: Option<String>,
	pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyOperationDef {
	pub policy_id: PolicyId,
	pub operation: String,
	pub body_source: String,
}

pub struct PolicyToCreate {
	pub name: Option<String>,
	pub target_type: PolicyTargetType,
	pub target_namespace: Option<String>,
	pub target_object: Option<String>,
	pub operations: Vec<PolicyOpToCreate>,
}

pub struct PolicyOpToCreate {
	pub operation: String,
	pub body_source: String,
}
