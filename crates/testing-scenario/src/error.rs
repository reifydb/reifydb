// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error,
	fmt::{Display, Formatter, Result},
};

#[derive(Debug, PartialEq, Eq)]
pub enum TestingScenarioError {
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

impl Display for TestingScenarioError {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result {
		match self {
			TestingScenarioError::NoQueries {
				scenario,
			} => write!(f, "scenario '{}' defines no queries", scenario),
			TestingScenarioError::NoProfiles {
				scenario,
			} => write!(f, "scenario '{}' defines no profiles", scenario),
			TestingScenarioError::DuplicateQuery {
				scenario,
				query,
			} => write!(f, "scenario '{}' defines query '{}' more than once", scenario, query),
			TestingScenarioError::DuplicateProfile {
				scenario,
				profile,
			} => write!(f, "scenario '{}' defines profile '{}' more than once", scenario, profile),
			TestingScenarioError::ScaledProfileOnManualDataset {
				scenario,
				profile,
			} => write!(
				f,
				"scenario '{}' has a manually populated dataset, so profile '{}' cannot request a row scale",
				scenario, profile
			),
			TestingScenarioError::FixedProfileOnGeneratedDataset {
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

impl Error for TestingScenarioError {}
