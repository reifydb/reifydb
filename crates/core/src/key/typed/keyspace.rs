// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use crate::{
	interface::store::CacheTiers,
	key::{
		operator_state::{GroupId, KeyspaceId},
		typed::{Key, layout::KeyLayout},
	},
};

pub trait Keyspace: Copy + Debug + 'static {
	const ID: KeyspaceId;
	const NAME: &'static str;
	const CACHE: CacheTiers;

	type Key: KeyLayout;
	type Suffix: Key;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix);

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key;
}
