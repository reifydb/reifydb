// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{Display, Formatter, Result as FmtResult},
	num::ParseIntError,
	str::FromStr,
};

use reifydb_type::value::uuid::Uuid7;
use uuid::Uuid as StdUuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchId(pub Uuid7);

impl Display for BatchId {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		write!(f, "{}", self.0.0.as_u128())
	}
}

impl FromStr for BatchId {
	type Err = ParseIntError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let n = s.parse::<u128>()?;
		Ok(BatchId(Uuid7(StdUuid::from_u128(n))))
	}
}
