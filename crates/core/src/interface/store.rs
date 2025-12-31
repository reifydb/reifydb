// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{CommitVersion, EncodedKey, value::encoded::EncodedValues};

#[derive(Debug, Clone)]
pub struct MultiVersionValues {
	pub key: EncodedKey,
	pub values: EncodedValues,
	pub version: CommitVersion,
}

#[derive(Debug, Clone)]
pub struct SingleVersionValues {
	pub key: EncodedKey,
	pub values: EncodedValues,
}
