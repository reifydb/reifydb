// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error,
	fmt::{Display, Formatter, Result},
};

#[derive(Debug, PartialEq, Eq)]
pub enum ScenarioError {
	NoQueries {
		scenario: String,
	},
	NoProfiles {
		scenario: String,
	},
	DuplicateQuery {
		scenario: String,
		query: String,
	},
	DuplicateProfile {
		scenario: String,
		profile: String,
	},
	ScaledProfileOnManualDataset {
		scenario: String,
		profile: String,
	},
	FixedProfileOnGeneratedDataset {
		scenario: String,
		profile: String,
	},
}

impl Display for ScenarioError {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result {
		match self {
			ScenarioError::NoQueries {
				scenario,
			} => write!(f, "scenario '{}' defines no queries", scenario),
			ScenarioError::NoProfiles {
				scenario,
			} => write!(f, "scenario '{}' defines no profiles", scenario),
			ScenarioError::DuplicateQuery {
				scenario,
				query,
			} => write!(f, "scenario '{}' defines query '{}' more than once", scenario, query),
			ScenarioError::DuplicateProfile {
				scenario,
				profile,
			} => write!(f, "scenario '{}' defines profile '{}' more than once", scenario, profile),
			ScenarioError::ScaledProfileOnManualDataset {
				scenario,
				profile,
			} => write!(
				f,
				"scenario '{}' has a manually populated dataset, so profile '{}' cannot request a row scale",
				scenario, profile
			),
			ScenarioError::FixedProfileOnGeneratedDataset {
				scenario,
				profile,
			} => write!(
				f,
				"scenario '{}' has a generated dataset, so profile '{}' must request a row scale",
				scenario, profile
			),
		}
	}
}

impl Error for ScenarioError {}
