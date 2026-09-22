// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	cast::{cast_column_data, convert::TargetConvert},
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{Value, identity::IdentityId, value_type::ValueType},
};

fn cast_any(values: Vec<Value>, target: ValueType) -> Result<ColumnBuffer> {
	cast_column_data(
		TargetConvert {
			target: None,
		},
		&ColumnBuffer::any(values),
		target,
		|| Fragment::internal("payload"),
	)
}

#[test]
fn an_identity_id_in_an_any_column_casts_to_an_identity_id_column_holding_it() {
	// An identity id in an Any column must cast to that same identity id, never panic in the builder.
	let id = IdentityId::anonymous();
	let cast = cast_any(vec![Value::IdentityId(id)], ValueType::IdentityId).expect("the cast must succeed");
	assert_eq!(cast.get_type(), ValueType::IdentityId);
	assert_eq!(cast.len(), 1);
	assert_eq!(cast.get_value(0), Value::IdentityId(id));
}
