// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
};

use crate::{
	error::Result,
	flow::operator::extern_c::binding::{
		context::ExternCOperatorContext,
		dictionary::{raw_find, raw_get, raw_id_by_name},
	},
};

pub struct Dictionary<'a> {
	ctx: &'a mut ExternCOperatorContext,
}

impl<'a> Dictionary<'a> {
	pub(crate) fn new(ctx: &'a mut ExternCOperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>> {
		raw_id_by_name(self.ctx, name)
	}

	pub fn find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>> {
		raw_find(self.ctx, dictionary, value)
	}

	pub fn get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>> {
		raw_get(self.ctx, dictionary, id)
	}
}
