// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt;

pub type IdentityId = u64;

#[derive(Debug, Clone)]
pub enum Identity {
	Anonymous {},
	System {
		id: IdentityId,
		name: String,
	},
	User {
		id: IdentityId,
		name: String,
	},
}

impl Identity {
	pub fn root() -> Self {
		Self::System {
			id: 0,
			name: "root".to_string(),
		}
	}
}

impl fmt::Display for Identity {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Anonymous {} => write!(f, "anonymous"),
			Self::System {
				id,
				..
			} => write!(f, "system:{}", id),
			Self::User {
				id,
				..
			} => write!(f, "user:{}", id),
		}
	}
}

use std::collections::HashMap;

pub trait AuthenticationProvider: Send + Sync {
	fn method(&self) -> &str;
	fn create(&self, config: &HashMap<String, String>) -> reifydb_type::Result<HashMap<String, String>>;
	fn validate(&self, stored: &HashMap<String, String>, credential: &str) -> reifydb_type::Result<bool>;
}
