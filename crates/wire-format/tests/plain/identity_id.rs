// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::identity_id::IdentityIdContainer, frame::data::FrameColumnData, identity::IdentityId, uuid::Uuid7,
};

fn make(v: Vec<IdentityId>) -> FrameColumnData {
	FrameColumnData::IdentityId(IdentityIdContainer::new(v))
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
