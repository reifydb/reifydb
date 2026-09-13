// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId},
};

use crate::resident::bucket::{BucketMap, write::WriteEntry};

impl BucketMap {
	pub fn for_each_entry(
		&self,
		operator: OperatorId,
		mut visit: impl FnMut(KeyspaceId, GroupId, &[u8], &WriteEntry),
	) {
		let mut ids = self.keyspaces_of(operator);
		ids.reverse();
		for keyspace in ids {
			let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
				continue;
			};
			bucket.for_each(&mut |group, suffix, entry| visit(keyspace, group, suffix, entry));
		}
	}
}
