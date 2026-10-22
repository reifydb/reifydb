// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::store::MultiVersionRow, key::any::TaggedKey};
use reifydb_value::Result;

pub trait Read {
	fn get(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<MultiVersionRow<TaggedKey>>>>;
	fn get_committed(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<MultiVersionRow<TaggedKey>>>>;
}
