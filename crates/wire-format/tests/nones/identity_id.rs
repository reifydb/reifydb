// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::identity_id::IdentityIdContainer, frame::data::FrameColumnData, identity::IdentityId, uuid::Uuid7,
	value_type::ValueType,
};

fn make(v: Vec<IdentityId>) -> FrameColumnData {
	FrameColumnData::IdentityId(IdentityIdContainer::new(v))
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
