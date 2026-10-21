// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::dictionary::Dictionary;
use reifydb_flow::txn::{Find, Resolve};
use reifydb_value::{
	Result,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

pub struct NoDictionaries;

impl Resolve for NoDictionaries {
	fn dictionary(&mut self, _id: DictionaryId) -> Result<Option<Dictionary>> {
		panic!("an operator over plain columns must never read a dictionary")
	}

	fn resolve(&mut self, _dictionary: &Dictionary, _ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>> {
		panic!("an operator over plain columns must never decode")
	}
}

impl Find for NoDictionaries {
	fn find(&mut self, _dictionary: &Dictionary, _values: &[Value]) -> Result<Vec<Option<DictionaryEntryId>>> {
		panic!("an operator over plain columns must never find a literal")
	}
}
