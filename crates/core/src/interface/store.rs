// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
