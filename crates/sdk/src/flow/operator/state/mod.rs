// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		operator::state::{OperatorState, decode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, KeyspaceMask},
	state::batch::{StateBatch, check_batch_keys},
};
use reifydb_value::error::Error as ValueError;

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		context::GuestBound,
		extern_c::binding::{context::ExternCContext, state as extern_c},
	},
};

pub struct State<'a> {
	ctx: &'a mut ExternCContext,
}

impl<'a> State<'a> {
	pub(crate) fn new(ctx: &'a mut ExternCContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn get<T: OperatorState>(&self, key: &GroupStateKey) -> Result<Option<T>> {
		match self.get_bytes(key)? {
			Some(row) => decode_payload(&row).map(Some),
			None => Ok(None),
		}
	}

	pub fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> Result<()> {
		let row = encode_payload(value)?;
		extern_c::set(self.ctx, key.as_encoded(), &row.into_bytes())
	}

	pub fn remove(&mut self, key: &GroupStateKey) -> Result<()> {
		extern_c::remove(self.ctx, key.as_encoded())
	}

	pub fn contains(&self, key: &GroupStateKey) -> Result<bool> {
		Ok(extern_c::get(self.ctx, key.as_encoded())?.is_some())
	}

	pub fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		match extern_c::get(self.ctx, key.as_encoded())? {
			Some(row) => Ok(Some(EncodedPodRow::from(row))),
			None => Ok(None),
		}
	}

	pub fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		extern_c::set(self.ctx, key.as_encoded(), &payload.into_bytes())
	}

	pub fn batch_bytes(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		check_batch_keys(&keys)?;
		let raw: Vec<EncodedKey> = keys.iter().map(|k| k.as_encoded().clone()).collect();
		let rows = extern_c::batch_read(self.ctx, &raw)?;
		Ok(StateBatch::read(keys, |_| Ok(rows.into_iter().map(|row| row.map(EncodedPodRow::from)).collect()))?)
	}

	pub fn write_batch_bytes(&mut self, batch: StateBatch) -> Result<()> {
		extern_c::batch_write(self.ctx, &batch)
	}

	pub fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		let raw: Vec<EncodedKey> = keys.iter().map(|k| k.as_encoded().clone()).collect();
		for (k, row) in extern_c::get_many(self.ctx, &raw)? {
			visit(framed(k)?, EncodedPodRow::from(row))?;
		}
		Ok(())
	}

	pub fn range_bytes_visit(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		for (seen, (k, row)) in
			extern_c::range(self.ctx, group, keyspace, start, end, limit.unwrap_or(usize::MAX))?
				.into_iter()
				.enumerate()
		{
			if limit.is_some_and(|l| seen >= l) {
				break;
			}
			visit(framed(k)?, EncodedPodRow::from(row))?;
		}
		Ok(())
	}

	pub fn sweep_many_bytes(
		&self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<(Vec<(GroupStateKey, EncodedPodRow)>, bool)> {
		let (rows, complete) =
			extern_c::sweep_many(self.ctx, groups, limit, KeyspaceMask::windowed().intersect(keyspaces))?;
		let mut framed_rows = Vec::with_capacity(rows.len());
		for (k, row) in rows {
			framed_rows.push((framed(k)?, EncodedPodRow::from(row)));
		}
		Ok((framed_rows, complete))
	}
}

#[inline]
fn framed(key: EncodedKey) -> Result<GroupStateKey> {
	match GroupStateKey::from_framed(key) {
		Some(key) => Ok(key),
		None => Err(SdkError::Serialization("host returned a state key that is not framed".to_string())),
	}
}

#[inline]
pub fn encode_payload<T: OperatorState>(value: &T) -> Result<EncodedPodRow> {
	Ok(value.encode_state().map_err(ValueError::from)?)
}

#[inline]
pub fn decode_payload<T: OperatorState>(row: &EncodedPodRow) -> Result<T> {
	Ok(decode(row).map_err(ValueError::from)?)
}
