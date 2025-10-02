// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::RowNumber;

use crate::value::encoded::{EncodedValues, EncodedValuesNamedLayout};

#[derive(Debug, Clone)]
pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedValues,
	pub layout: EncodedValuesNamedLayout,
}
