// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, GroupStateKey},
};
use smallvec::SmallVec;

use crate::{
	bound::{span, split_bound},
	resident::bucket::{BucketMap, GroupIds, write::WriteEntry},
	types::Scan,
};

impl BucketMap {
	fn groups_of(&self, operator: OperatorId, lower: Bound<GroupId>, upper: Bound<GroupId>) -> GroupIds {
		let mut ids = GroupIds::new();
		for keyspace in self.keyspaces_of(operator) {
			let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
				continue;
			};
			ids.extend(bucket.groups_in_range(&lower, &upper));
		}
		ids.sort_by(|left, right| right.cmp(left));
		ids.dedup();
		ids
	}

	pub fn encoded_range(
		&self,
		operator: OperatorId,
		lower: &Bound<EncodedKey>,
		upper: &Bound<EncodedKey>,
		scan: Scan,
		limit: usize,
	) -> Vec<(GroupStateKey, WriteEntry)> {
		self.range_of(operator, lower, upper, scan, limit, false)
	}

	pub fn encoded_range_shadowed(
		&self,
		operator: OperatorId,
		lower: &Bound<EncodedKey>,
		upper: &Bound<EncodedKey>,
		scan: Scan,
		limit: usize,
	) -> Vec<(GroupStateKey, WriteEntry)> {
		self.range_of(operator, lower, upper, scan, limit, true)
	}

	fn range_of(
		&self,
		operator: OperatorId,
		lower: &Bound<EncodedKey>,
		upper: &Bound<EncodedKey>,
		scan: Scan,
		limit: usize,
		tombstones: bool,
	) -> Vec<(GroupStateKey, WriteEntry)> {
		let (start, start_group, start_at) = split_bound(lower.as_ref());
		let (end, end_group, end_at) = split_bound(upper.as_ref());
		let end_open = matches!(end, Bound::Excluded(ref suffix) if suffix.is_empty());

		if let (Some(low), Some(high)) = (end_group, start_group)
			&& low > high
		{
			return Vec::new();
		}
		let lower = end_group.map_or(Bound::Unbounded, Bound::Included);
		let upper = start_group.map_or(Bound::Unbounded, Bound::Included);

		let mut groups = self.groups_of(operator, lower, upper);
		if scan == Scan::Backward {
			groups.reverse();
		}

		let mut chunks: SmallVec<[Vec<(GroupStateKey, WriteEntry)>; 4]> = SmallVec::new();
		let mut taken = 0usize;
		'groups: for group in groups {
			let opens = start_group == Some(group);
			let closes = end_group == Some(group);
			let mut ids = span(
				opens.then_some(start_at).flatten(),
				closes.then_some(end_at).flatten(),
				closes && end_open,
			);
			if scan == Scan::Backward {
				ids.reverse();
			}
			for keyspace in ids {
				let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
					continue;
				};
				let from = match opens && start_at == Some(keyspace) {
					true => start.clone(),
					false => Bound::Unbounded,
				};
				let to = match closes && end_at == Some(keyspace) {
					true => end.clone(),
					false => Bound::Unbounded,
				};
				let chunk = bucket.encoded_range_in(group, &from, &to, scan, limit - taken, tombstones);
				taken += match tombstones {
					true => chunk.iter().filter(|(_, entry)| entry.post.is_some()).count(),
					false => chunk.len(),
				};
				chunks.push(chunk);
				if taken >= limit {
					break 'groups;
				}
			}
		}
		if scan == Scan::Backward {
			chunks.reverse();
		}
		chunks.into_iter().flatten().collect()
	}
}
