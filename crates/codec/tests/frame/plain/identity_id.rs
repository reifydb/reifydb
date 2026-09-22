// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::uuid_array::identity_id_array, frame::data::FrameColumnData, identity::IdentityId, uuid::Uuid7,
};

fn make(v: Vec<IdentityId>) -> FrameColumnData {
	FrameColumnData::IdentityId(identity_id_array(v))
}

crate::plain_tests! {
	typical: vec![
		IdentityId::new(Uuid7(uuid::Uuid::nil())),
		IdentityId::new(Uuid7(uuid::Uuid::max())),
	],
	boundary: vec![
		IdentityId::new(Uuid7(uuid::Uuid::nil())),
		IdentityId::new(Uuid7(uuid::Uuid::max())),
	],
	single: IdentityId::new(Uuid7(uuid::Uuid::nil())),
}
