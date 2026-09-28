// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::ValueKind;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_sdk::common::extern_wasm::{
	layout::{EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn},
	marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes},
};
use reifydb_value::fragment::Fragment;

fn guest_bytes_of_kind(kind: ValueKind) -> Vec<u8> {
	// Only the type code may differ from valid guest bytes, otherwise the error could come from bad data instead.
	let columns = Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), ColumnBuffer::int4([1, 2]))]);
	let mut bytes = marshal_columns_to_bytes(&columns).unwrap();
	let mut descriptor = ExternWasmColumn::read_from_bytes(&bytes[EXTERN_WASM_COLUMNS_HEADER_SIZE..]);
	descriptor.type_code = kind.byte();
	descriptor.write_at(&mut bytes, EXTERN_WASM_COLUMNS_HEADER_SIZE);
	bytes
}

#[test]
fn guest_columns_the_wasm_marshal_can_not_carry_fail_to_unmarshal() {
	// Otherwise every value of such a guest column is silently replaced by a none.
	let wrong: Vec<String> =
		[ValueKind::List, ValueKind::Type, ValueKind::Record, ValueKind::Tuple, ValueKind::Digest]
			.into_iter()
			.filter_map(|kind| {
				let expected = format!("guest {kind:?} column is not supported by the wasm marshal");
				match unmarshal_columns_from_bytes(&guest_bytes_of_kind(kind)) {
					Err(err) if err.to_string().contains(&expected) => None,
					Err(err) => Some(format!("{kind:?}: failed with the wrong error: {err}")),
					Ok(columns) => Some(format!(
						"{kind:?}: unmarshalled {} rows without an error",
						columns.row_count()
					)),
				}
			})
			.collect();
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
