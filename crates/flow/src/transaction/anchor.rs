// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{
	key::{
		decode_u64_asc, encode_u64_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
	row::{
		bytes::EncodedBytes,
		operator::{EncodedOperatorRow, OperatorState},
	},
};
use reifydb_core::{
	actors::pending::PendingWrite,
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{
			GroupId, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range, node_prefix,
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

pub const ANCHOR_SUFFIX_LEN: usize = 9;

pub const UNGROUPED_SIDE: u8 = 0xFF;

#[operator_state]
#[derive(Clone)]
pub struct SealAnchor {
	pub expiry: DateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealPage {
	pub due: Vec<(u8, RowNumber)>,
	pub next: Option<DateTime>,
	pub more: bool,
}

pub fn anchor_key(group: GroupId, side: u8, row_number: RowNumber) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(ANCHOR_SUFFIX_LEN);
	suffix.push(side);
	suffix.extend_from_slice(&encode_u64_asc(row_number.0));
	OperatorStateKey::inner_encoded(group, Keyspace::SEAL_ANCHOR, suffix)
}

pub fn anchor_range(group: GroupId) -> EncodedKeyRange {
	keyspace_inner_range(group, Keyspace::SEAL_ANCHOR)
}

pub fn decode_anchor_suffix(suffix: &[u8]) -> Option<(u8, RowNumber)> {
	let tail = <[u8; 8]>::try_from(suffix.get(1..)?).ok()?;
	Some((suffix[0], RowNumber(decode_u64_asc(tail))))
}

pub fn decode_anchor_expiry(bytes: &EncodedBytes) -> Result<DateTime> {
	let row = EncodedOperatorRow::try_from(bytes.clone()).map_err(ValueError::from)?;
	Ok(SealAnchor::decode_state(&row).map_err(ValueError::from)?.expiry)
}

struct ShadowEntry {
	side: u8,
	row_number: RowNumber,
	expiry: Option<DateTime>,
}

pub trait SealAnchorExtension: FlowTransaction {
	fn anchor_shadow(&self, id: OperatorId, group: GroupId) -> Result<BTreeMap<EncodedKey, PendingWrite>> {
		let range = anchor_range(group).with_prefix(EncodedKey::new(node_prefix(id)));
		let mut out = BTreeMap::new();
		self.pending_layers().collect_range((range.start.as_ref(), range.end.as_ref()), &mut out);
		Ok(out)
	}

	fn anchor_at(
		&mut self,
		id: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Result<Option<DateTime>> {
		let key = scoped_key(id, &anchor_key(group, side, row_number));
		match self.lookup_overlays(&key) {
			Some(None) => Ok(None),
			Some(Some(bytes)) => decode_anchor_expiry(&bytes).map(Some),
			None => Ok(self.operator_store().anchor_get(id, group, side, row_number)),
		}
	}

	fn anchor_min(&mut self, id: OperatorId, group: GroupId) -> Result<Option<DateTime>> {
		let shadow = self.anchor_shadow(id, group)?;
		let mut best: Option<DateTime> = None;
		for entry in decode_shadow(&shadow)? {
			if let Some(expiry) = entry.expiry {
				best = Some(best.map_or(expiry, |current: DateTime| current.min(expiry)));
			}
		}

		let limit = shadow.len() + 1;
		let rows = self.operator_store().anchors_by_expiry(id, group, limit as u64);
		#[cfg(reifydb_assertions)]
		let returned = rows.len();
		let unshadowed = rows.into_iter().find(|anchor| {
			!shadow.contains_key(&scoped_key(id, &anchor_key(group, anchor.side, anchor.row_number)))
		});
		if let Some(anchor) = &unshadowed {
			best = Some(best.map_or(anchor.expiry, |current: DateTime| current.min(anchor.expiry)));
		}
		reifydb_assertions! {
			assert!(
				unshadowed.is_some() || returned < limit,
				"a full page of {returned} anchors was entirely shadowed by {} pending writes, so \
				 the group's true earliest expiry was never read",
				shadow.len()
			);
		}

		Ok(best)
	}

	fn anchor_seal_page(
		&mut self,
		id: OperatorId,
		group: GroupId,
		at: DateTime,
		budget: usize,
	) -> Result<SealPage> {
		let shadow = self.anchor_shadow(id, group)?;
		let limit = shadow.len() + budget + 1;
		let rows = self.operator_store().anchors_by_expiry(id, group, limit as u64);
		let capped = rows.len() == limit;

		let mut merged: Vec<(DateTime, u8, RowNumber)> = decode_shadow(&shadow)?
			.into_iter()
			.filter_map(|entry| entry.expiry.map(|expiry| (expiry, entry.side, entry.row_number)))
			.collect();
		for anchor in rows {
			if shadow.contains_key(&scoped_key(id, &anchor_key(group, anchor.side, anchor.row_number))) {
				continue;
			}
			merged.push((anchor.expiry, anchor.side, anchor.row_number));
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

		Ok(SealPage {
			due,
			next,
			more: more || (next.is_none() && capped),
		})
	}
}

impl<T: FlowTransaction> SealAnchorExtension for T {}

fn decode_shadow(shadow: &BTreeMap<EncodedKey, PendingWrite>) -> Result<Vec<ShadowEntry>> {
	let mut out = Vec::with_capacity(shadow.len());
	for (key, write) in shadow {
		let Some((side, row_number)) =
			OperatorStateKey::decode(key).and_then(|decoded| decode_anchor_suffix(&decoded.suffix))
		else {
			continue;
		};
		let expiry = match write {
			PendingWrite::Set(bytes) => Some(decode_anchor_expiry(bytes)?),
			PendingWrite::Remove {
				..
			} => None,
		};
		out.push(ShadowEntry {
			side,
			row_number,
			expiry,
		});
	}
	Ok(out)
}
