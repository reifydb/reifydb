// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

pub type PolicyId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataOp {
	From,
	Insert,
	Update,
	Delete,
}

impl DataOp {
	pub const ALL: &'static [DataOp] = &[DataOp::From, DataOp::Insert, DataOp::Update, DataOp::Delete];

	pub fn as_str(&self) -> &'static str {
		match self {
			Self::From => "from",
			Self::Insert => "insert",
			Self::Update => "update",
			Self::Delete => "delete",
		}
	}

	pub fn parse(s: &str) -> Option<Self> {
		match s {
			"from" => Some(Self::From),
			"insert" => Some(Self::Insert),
			"update" => Some(Self::Update),
			"delete" => Some(Self::Delete),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallableOp {
	Call,
}

impl CallableOp {
	pub const ALL: &'static [CallableOp] = &[CallableOp::Call];

	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Call => "call",
		}
	}

	pub fn parse(s: &str) -> Option<Self> {
		match s {
			"call" => Some(Self::Call),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionOp {
	Admin,
	Command,
	Query,
	Subscription,
}

impl SessionOp {
	pub const ALL: &'static [SessionOp] =
		&[SessionOp::Admin, SessionOp::Command, SessionOp::Query, SessionOp::Subscription];

	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Admin => "admin",
			Self::Command => "command",
			Self::Query => "query",
			Self::Subscription => "subscription",
		}
	}

	pub fn parse(s: &str) -> Option<Self> {
		match s {
			"admin" => Some(Self::Admin),
			"command" => Some(Self::Command),
			"query" => Some(Self::Query),
			"subscription" => Some(Self::Subscription),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PolicyTargetType {
	Table,
	Column,
	Namespace,
	Procedure,
	Function,
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
			Self::Subscription => "subscription",
			Self::Series => "series",
			Self::Dictionary => "dictionary",
			Self::Session => "session",
			Self::Feature => "feature",
			Self::View => "view",
			Self::RingBuffer => "ringbuffer",
		}
	}

	pub fn is_valid_operation(&self, op: &str) -> bool {
		match self {
			Self::Table
			| Self::View
			| Self::Series
			| Self::RingBuffer
			| Self::Dictionary
			| Self::Column
			| Self::Namespace => DataOp::parse(op).is_some(),
			Self::Procedure | Self::Function => CallableOp::parse(op).is_some(),
			Self::Session => SessionOp::parse(op).is_some(),
			Self::Subscription | Self::Feature => false,
		}
	}

	pub fn valid_operation_names(&self) -> &'static [&'static str] {
		match self {
			Self::Table
			| Self::View
			| Self::Series
			| Self::RingBuffer
			| Self::Dictionary
			| Self::Column
			| Self::Namespace => &["from", "insert", "update", "delete"],
			Self::Procedure | Self::Function => &["call"],
			Self::Session => &["admin", "command", "query", "subscription"],
			Self::Subscription | Self::Feature => &[],
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Policy {
	pub id: PolicyId,
	pub name: Option<String>,
	pub target_type: PolicyTargetType,
	pub target_namespace: Option<String>,
	pub target_shape: Option<String>,
	pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyOperation {
	pub policy_id: PolicyId,
	pub operation: String,
	pub body_source: String,
}

pub struct PolicyToCreate {
	pub name: Option<String>,
	pub target_type: PolicyTargetType,
	pub target_namespace: Option<String>,
	pub target_shape: Option<String>,
	pub operations: Vec<PolicyOpToCreate>,
}

pub struct PolicyOpToCreate {
	pub operation: String,
	pub body_source: String,
}
