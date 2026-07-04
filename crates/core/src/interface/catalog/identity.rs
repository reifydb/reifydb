// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{identity::IdentityId, value_type::ValueType};
use serde::{Deserialize, Serialize};

pub type RoleId = u64;

pub type IdentityAttributeId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
	pub id: IdentityId,
	pub name: String,
	pub enabled: bool,
}

impl Identity {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Role {
	pub id: RoleId,
	pub name: String,
}

impl Role {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GrantedRole {
	pub identity: IdentityId,
	pub role_id: RoleId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdentityAttribute {
	pub id: IdentityAttributeId,
	pub name: String,
	pub value_type: ValueType,
}

impl IdentityAttribute {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdentityAttributeValue {
	pub identity: IdentityId,
	pub attribute: IdentityAttributeId,
	pub value: String,
}
