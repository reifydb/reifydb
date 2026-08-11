// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod extern_c;

use extern_c::{raw_find, raw_get, raw_id_by_name};
use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
};

use crate::{error::Result, operator::context::extern_c::ExternCOperatorContext};

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

#[cfg(test)]
mod tests {
	use std::{ffi::c_void, ptr::null};

	use reifydb_abi::context::context::ExternCContext;
	use reifydb_core::common::CommitVersion;
	use reifydb_testing_sdk::{callbacks::create_test_callbacks, context::TestContext};
	use reifydb_value::value::{Value, dictionary::DictionaryId, value_type::ValueType};

	use crate::operator::context::extern_c::ExternCOperatorContext;

	#[test]
	fn dictionary_round_trips_through_extern_c() {
		let test_ctx = TestContext::new(CommitVersion(1));
		test_ctx.seed_dictionary(
			"solana::mints",
			7,
			ValueType::Uint4,
			&[(1, Value::Utf8("MINTA".to_string())), (2, Value::Utf8("MINTB".to_string()))],
		);

		let mut extern_c_context = ExternCContext {
			txn_ptr: &test_ctx as *const TestContext as *mut c_void,
			executor_ptr: null(),
			operator_id: 1,
			written_at_nanos: 0,
			callbacks: create_test_callbacks(),
		};
		let mut ctx = ExternCOperatorContext::new(&mut extern_c_context as *mut ExternCContext);

		let id = ctx.dictionary().id_by_name("solana::mints").unwrap().expect("dictionary id");
		assert_eq!(id, DictionaryId(7));

		let entry = ctx.dictionary().find(id, &Value::Utf8("MINTA".to_string())).unwrap().expect("entry id");
		let decoded = ctx.dictionary().get(id, entry).unwrap().expect("decoded value");
		assert_eq!(decoded, Value::Utf8("MINTA".to_string()));

		assert!(ctx.dictionary().find(id, &Value::Utf8("MISSING".to_string())).unwrap().is_none());
		assert!(ctx.dictionary().id_by_name("solana::unknown").unwrap().is_none());
	}
}
