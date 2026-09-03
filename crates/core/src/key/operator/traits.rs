// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use crate::{
	interface::store::CacheTiers,
	key::{
		operator::state::{GroupId, KeyspaceId},
		typed::layout::KeyLayout,
	},
};

pub trait Keyspace: Copy + Debug + 'static {
	const ID: KeyspaceId;
	const NAME: &'static str;
	const CACHE: CacheTiers;

	type GroupedKey: KeyLayout;
	type Suffix: KeyLayout;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix);

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey;
}

pub const fn group_scoped<K: Keyspace>() -> bool {
	let key = <K::GroupedKey as KeyLayout>::COLUMNS;
	let suffix = <K::Suffix as KeyLayout>::COLUMNS;
	key.len() == suffix.len() + 1 && leads_on_group(key[0].name)
}

const fn leads_on_group(name: &str) -> bool {
	let bytes = name.as_bytes();
	bytes.len() == 5
		&& bytes[0] == b'g'
		&& bytes[1] == b'r'
		&& bytes[2] == b'o'
		&& bytes[3] == b'u'
		&& bytes[4] == b'p'
}
