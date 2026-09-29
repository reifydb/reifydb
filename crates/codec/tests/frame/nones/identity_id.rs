// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::uuid_array::identity_id_array, identity::IdentityId, uuid::Uuid7, value_type::ValueType,
};

use crate::common::{ColumnData, data};

fn make(v: Vec<IdentityId>) -> ColumnData {
	data(ValueType::IdentityId, identity_id_array(v))
}

crate::nones_tests! {
	values: vec![
		IdentityId::new(Uuid7(uuid::Uuid::nil())),
		IdentityId::new(Uuid7(uuid::Uuid::max())),
		IdentityId::new(Uuid7(uuid::Uuid::from_u128(1))),
		IdentityId::new(Uuid7(uuid::Uuid::from_u128(42))),
		IdentityId::new(Uuid7(uuid::Uuid::from_u128(u128::MAX - 1))),
	],
	inner_type: ValueType::IdentityId,
}
