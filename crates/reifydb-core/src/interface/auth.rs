// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub type PrincipalId = u64;

#[derive(Debug, Clone)]
pub enum Principal {
	Anonymous {},
	System {
		id: PrincipalId,
		name: String,
	},
	User {
		id: PrincipalId,
		name: String,
	},
}

impl Principal {
	pub fn root() -> Self {
		Self::System {
			id: 0,
			name: "root".to_string(),
		}
	}
}
