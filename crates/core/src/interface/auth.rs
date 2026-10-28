// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	result::Result as StdResult,
};

use reifydb_runtime::context::rng::Rng;
use reifydb_value::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MethodSettingError {
	Unknown {
		key: String,
	},
	Missing {
		key: String,
	},
	HostSecretMissing {
		key: String,
	},
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthStep {
	Authenticated,

	Failed,

	Rejected {
		reason: String,
	},

	Challenge {
		payload: HashMap<String, String>,
	},
}

pub trait AuthenticationProvider: Send + Sync {
	fn method(&self) -> &str;

	fn create(&self, rng: &Rng, config: &HashMap<String, String>) -> Result<BTreeMap<String, String>>;

	fn authenticate(
		&self,
		stored: &BTreeMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep>;

	fn configure_method(
		&self,
		props: &BTreeMap<String, String>,
	) -> StdResult<BTreeMap<String, String>, MethodSettingError> {
		match props.keys().next() {
			Some(key) => Err(MethodSettingError::Unknown {
				key: key.clone(),
			}),
			None => Ok(BTreeMap::new()),
		}
	}

	fn can_promote_guest(&self) -> bool {
		false
	}

	fn verify_challenge(
		&self,
		_stored: &BTreeMap<String, String>,
		_challenge: &HashMap<String, String>,
		_credentials: &HashMap<String, String>,
	) -> Result<AuthStep> {
		Ok(AuthStep::Failed)
	}
}
