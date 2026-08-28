// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{
	key::{
		decode_u64_asc, encode_u64_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
	row::{bytes::EncodedBytes, operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	actors::pending::PendingWrite,
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{
			GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, keyspace_inner_range, node_prefix,
		},
	},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	error::Error as ValueError,
	reifydb_assertions,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::transaction::{FlowTransaction, scope::scoped_key};

pub const JOIN_EXPIRY_SUFFIX_LEN: usize = 9;

#[operator_state]
#[derive(Clone)]
pub struct JoinRowExpiry {
	pub at: DateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinDuePage {
	pub due: Vec<(u8, RowNumber)>,
	pub next: Option<DateTime>,
	pub more: bool,
}

pub fn join_expiry_key(group: GroupId, side: u8, row_number: RowNumber) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(JOIN_EXPIRY_SUFFIX_LEN);
	suffix.push(side);
	suffix.extend_from_slice(&encode_u64_asc(row_number.0));
	OperatorStateKey::inner_encoded(group, KeyspaceId::JOIN_ROW_EXPIRY, suffix)
}

pub fn join_expiry_range(group: GroupId) -> EncodedKeyRange {
	keyspace_inner_range(group, KeyspaceId::JOIN_ROW_EXPIRY)
}

pub fn decode_join_expiry_suffix(suffix: &[u8]) -> Option<(u8, RowNumber)> {
	let tail = <[u8; 8]>::try_from(suffix.get(1..)?).ok()?;
	Some((suffix[0], RowNumber(decode_u64_asc(tail))))
}

pub fn decode_join_expiry(bytes: &EncodedBytes) -> Result<DateTime> {
	let row = EncodedPodRow::from(bytes.clone());
	Ok(JoinRowExpiry::decode_state(&row).map_err(ValueError::from)?.at)
}

struct ShadowEntry {
	side: u8,
	row_number: RowNumber,
	at: Option<DateTime>,
}

pub trait JoinRowExpiryExtension: FlowTransaction {
	fn join_expiry_shadow(&self, id: OperatorId, group: GroupId) -> Result<BTreeMap<EncodedKey, PendingWrite>> {
		let range = join_expiry_range(group).with_prefix(EncodedKey::new(node_prefix(id)));
		let mut out = BTreeMap::new();
		self.pending_layers().collect_range((range.start.as_ref(), range.end.as_ref()), &mut out);
		Ok(out)
	}

	fn join_expiry_at(
		&mut self,
		id: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Result<Option<DateTime>> {
		let key = scoped_key(id, &join_expiry_key(group, side, row_number));
		match self.lookup_overlays(&key) {
			Some(None) => Ok(None),
			Some(Some(bytes)) => decode_join_expiry(&bytes).map(Some),
			None => Ok(self.operator_store().join_expiry_get(id, group, side, row_number)),
		}
	}

	fn join_expiry_min(&mut self, id: OperatorId, group: GroupId) -> Result<Option<DateTime>> {
		let shadow = self.join_expiry_shadow(id, group)?;
		let mut best: Option<DateTime> = None;
		for entry in decode_shadow(&shadow)? {
			if let Some(at) = entry.at {
				best = Some(best.map_or(at, |current: DateTime| current.min(at)));
			}
		}

		let limit = shadow.len() + 1;
		let rows = self.operator_store().join_expiries_by_time(id, group, limit as u64);
		#[cfg(reifydb_assertions)]
		let returned = rows.len();
		let unshadowed = rows.into_iter().find(|row| {
			!shadow.contains_key(&scoped_key(id, &join_expiry_key(group, row.side, row.row_number)))
		});
		if let Some(row) = &unshadowed {
			best = Some(best.map_or(row.at, |current: DateTime| current.min(row.at)));
		}
		reifydb_assertions! {
			assert!(
				unshadowed.is_some() || returned < limit,
				"a full page of {returned} join expiries was entirely shadowed by {} pending writes, so \
				 the group's true earliest expiry was never read",
				shadow.len()
			);
		}

		Ok(best)
	}

	fn join_due_page(
		&mut self,
		id: OperatorId,
		group: GroupId,
		at: DateTime,
		budget: usize,
	) -> Result<JoinDuePage> {
		let shadow = self.join_expiry_shadow(id, group)?;
		let limit = shadow.len() + budget + 1;
		let rows = self.operator_store().join_expiries_by_time(id, group, limit as u64);
		let capped = rows.len() == limit;

		let mut merged: Vec<(DateTime, u8, RowNumber)> = decode_shadow(&shadow)?
			.into_iter()
			.filter_map(|entry| entry.at.map(|expiry| (expiry, entry.side, entry.row_number)))
			.collect();
		for row in rows {
			if shadow.contains_key(&scoped_key(id, &join_expiry_key(group, row.side, row.row_number))) {
				continue;
			}
			merged.push((row.at, row.side, row.row_number));
		}
		merged.sort_unstable();

		let mut due = Vec::new();
		let mut next = None;
		let mut more = false;
		for (expiry, side, row_number) in merged {
			if expiry > at {
				next = Some(expiry);
				break;
			}
			if due.len() == budget {
				more = true;
				break;
			}
			due.push((side, row_number));
		}

		Ok(JoinDuePage {
			due,
			next,
			more: more || (next.is_none() && capped),
		})
	}
}

impl<T: FlowTransaction> JoinRowExpiryExtension for T {}

fn decode_shadow(shadow: &BTreeMap<EncodedKey, PendingWrite>) -> Result<Vec<ShadowEntry>> {
	let mut out = Vec::with_capacity(shadow.len());
	for (key, write) in shadow {
		let Some((side, row_number)) =
			OperatorStateKey::decode(key).and_then(|decoded| decode_join_expiry_suffix(&decoded.suffix))
		else {
			continue;
		};
		let at = match write {
			PendingWrite::Set(bytes) => Some(decode_join_expiry(bytes)?),
			PendingWrite::Remove {
				..
			} => None,
		};
		out.push(ShadowEntry {
			side,
			row_number,
			at,
		});
	}
	Ok(out)
}
