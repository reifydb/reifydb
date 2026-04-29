// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::{identity::IdentityId, uuid::Uuid7};
use uuid::Uuid;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn identity_id_anonymous() {
	let input = ColumnBuffer::identity_id([IdentityId::anonymous()]);
	let output = round_trip_column("id", input.clone());
	assert_column_eq("identity_id_anonymous", &input, &output);
}

#[test]
fn identity_id_root() {
	let input = ColumnBuffer::identity_id([IdentityId::root()]);
	let output = round_trip_column("id", input.clone());
	assert_column_eq("identity_id_root", &input, &output);
}

#[test]
fn identity_id_system() {
	let input = ColumnBuffer::identity_id([IdentityId::system()]);
	let output = round_trip_column("id", input.clone());
	assert_column_eq("identity_id_system", &input, &output);
}

#[test]
fn identity_id_specific_known_bytes() {
	let bytes = [0x01, 0x8D, 0x5E, 0x30, 0x4B, 0x78, 0x7A, 0xBC, 0x91, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
	let input = ColumnBuffer::identity_id([IdentityId::new(Uuid7(Uuid::from_bytes(bytes)))]);
	let output = round_trip_column("id", input.clone());
	assert_column_eq("identity_id_specific", &input, &output);
}

#[test]
fn identity_id_thirty_two_rows() {
	let values: Vec<IdentityId> = (0..32u8)
		.map(|i| {
			let mut bytes = [0u8; 16];
			for (j, b) in bytes.iter_mut().enumerate() {
				*b = i.wrapping_mul(j as u8 + 1).wrapping_add(j as u8);
			}
			IdentityId::new(Uuid7(Uuid::from_bytes(bytes)))
		})
		.collect();
	let input = ColumnBuffer::identity_id(values);
	let output = round_trip_column("id", input.clone());
	assert_column_eq("identity_id_thirty_two", &input, &output);
}

#[test]
fn identity_id_with_undefined() {
	let input = ColumnBuffer::identity_id_optional([
		Some(IdentityId::root()),
		None,
		Some(IdentityId::system()),
		None,
		Some(IdentityId::anonymous()),
	]);
	let output = round_trip_column("id", input.clone());
	assert_column_eq("identity_id_with_undefined", &input, &output);
}
