// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
