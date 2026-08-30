// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod utils;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		operator::state::{OperatorState, decode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::key::operator::state::{GroupId, GroupStateKey, KeyspaceId};
use reifydb_value::error::Error as ValueError;

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		context::{GuestContext, GuestState, GuestBound},
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

	pub fn clear(&mut self) -> Result<()> {
		extern_c::clear(self.ctx)
	}

	pub fn scan_prefix<T: OperatorState>(&self, prefix: &GroupStateKey) -> Result<Vec<(GroupStateKey, T)>> {
		extern_c::prefix(self.ctx, prefix.as_encoded(), usize::MAX)?
			.into_iter()
			.map(|(k, row)| Ok((framed(k)?, decode_payload(&EncodedPodRow::from(row))?)))
			.collect()
	}

	pub fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, T)>> {
		let raw: Vec<EncodedKey> = keys.iter().map(|k| k.as_encoded().clone()).collect();
		extern_c::get_many(self.ctx, &raw)?
			.into_iter()
			.map(|(k, row)| Ok((framed(k)?, decode_payload(&EncodedPodRow::from(row))?)))
			.collect()
	}

	pub fn keys_with_prefix(&self, prefix: &GroupStateKey) -> Result<Vec<GroupStateKey>> {
		extern_c::prefix(self.ctx, prefix.as_encoded(), usize::MAX)?
			.into_iter()
			.map(|(k, _)| framed(k))
			.collect()
	}

	pub fn range<T: OperatorState>(
		&self,
		group: GroupId,
		keyspace: KeyspaceId,
		start: GuestBound<'_>,
		end: GuestBound<'_>,
	) -> Result<Vec<(GroupStateKey, T)>> {
		extern_c::range(self.ctx, group, keyspace, start, end, usize::MAX)?
			.into_iter()
			.map(|(k, row)| Ok((framed(k)?, decode_payload(&EncodedPodRow::from(row))?)))
			.collect()
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
		for (seen, (k, row)) in extern_c::range(self.ctx, group, keyspace, start, end, limit.unwrap_or(usize::MAX))?
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

pub trait GuestRawOperator {
	fn state_get<T: OperatorState>(&self, ctx: &mut impl GuestContext, key: &GroupStateKey) -> Result<Option<T>> {
		ctx.state().get(key)
	}

	fn state_set<T: OperatorState>(
		&self,
		ctx: &mut impl GuestContext,
		key: &GroupStateKey,
		value: &T,
	) -> Result<()> {
		ctx.state().set(key, value)
	}

	fn state_remove(&self, ctx: &mut impl GuestContext, key: &GroupStateKey) -> Result<()> {
		ctx.state().remove(key)
	}

	fn state_clear(&self, ctx: &mut impl GuestContext) -> Result<()> {
		ctx.state().clear()
	}
}
