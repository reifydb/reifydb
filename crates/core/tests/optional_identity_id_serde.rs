// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{identity::IdentityId, uuid::Uuid7};
use serde_json::{from_str, to_string};
use uuid::Uuid;

fn column_with_a_none_row() -> ColumnBuffer {
	let id = IdentityId(Uuid7(Uuid::from_u128(0x0000_0000_0001_7000_8000_0000_0000_0000)));
	ColumnBuffer::identity_id_with_bitvec([id, IdentityId::default()], vec![true, false])
}

#[test]
fn optional_identity_id_column_with_a_none_row_round_trips_through_postcard() {
	// The placeholder under a none row must never make the whole column fail to load.
	let column = column_with_a_none_row();
	let bytes = to_stdvec(&column).expect("the column serializes");
	let loaded: ColumnBuffer = from_bytes(&bytes).expect("the column must load back");
	assert_eq!(loaded, column);
}

#[test]
fn optional_identity_id_column_with_a_none_row_round_trips_through_json() {
	// The placeholder under a none row must never make the whole column fail to load.
	let column = column_with_a_none_row();
	let json = to_string(&column).expect("the column serializes");
	let loaded: ColumnBuffer = from_str(&json).expect("the column must load back");
	assert_eq!(loaded, column);
}
