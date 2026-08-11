// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod extern_c;
pub mod utils;

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::operator::{EncodedOperatorRow, OperatorState, decode},
};
use reifydb_core::key::operator_state::GroupStateKey;
use reifydb_value::{error::Error as ValueError, value::datetime::DateTime};

use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi, extern_c::ExternCOperatorContext},
};

pub struct State<'a> {
	ctx: &'a mut ExternCOperatorContext,
}

impl<'a> State<'a> {
	pub(crate) fn new(ctx: &'a mut ExternCOperatorContext) -> Self {
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
		let row = encode_payload(value, self.written_at())?;
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
		extern_c::prefix(self.ctx, prefix.as_encoded())?
			.into_iter()
			.filter_map(|(k, row)| GroupStateKey::from_framed(k).map(|k| (k, row)))
			.map(|(k, row)| {
				Ok((k, decode_payload(&EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)?))
			})
			.collect()
	}

	pub fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, T)>> {
		let raw: Vec<EncodedKey> = keys.iter().map(|k| k.as_encoded().clone()).collect();
		extern_c::get_many(self.ctx, &raw)?
			.into_iter()
			.filter_map(|(k, row)| GroupStateKey::from_framed(k).map(|k| (k, row)))
			.map(|(k, row)| {
				Ok((k, decode_payload(&EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)?))
			})
			.collect()
	}

	pub fn keys_with_prefix(&self, prefix: &GroupStateKey) -> Result<Vec<GroupStateKey>> {
		Ok(extern_c::prefix(self.ctx, prefix.as_encoded())?
			.into_iter()
			.filter_map(|(k, _)| GroupStateKey::from_framed(k))
			.collect())
	}

	pub fn range<T: OperatorState>(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
	) -> Result<Vec<(GroupStateKey, T)>> {
		extern_c::range(self.ctx, start.map(GroupStateKey::as_encoded), end.map(GroupStateKey::as_encoded))?
			.into_iter()
			.filter_map(|(k, row)| GroupStateKey::from_framed(k).map(|k| (k, row)))
			.map(|(k, row)| {
				Ok((k, decode_payload(&EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)?))
			})
			.collect()
	}

	pub fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		match extern_c::get(self.ctx, key.as_encoded())? {
			Some(row) => Ok(Some(EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)),
			None => Ok(None),
		}
	}

	pub fn set_bytes(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()> {
		extern_c::set(self.ctx, key.as_encoded(), &payload.into_bytes())
	}

	pub fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		let raw: Vec<EncodedKey> = keys.iter().map(|k| k.as_encoded().clone()).collect();
		for (k, row) in extern_c::get_many(self.ctx, &raw)? {
			let Some(k) = GroupStateKey::from_framed(k) else {
				continue;
			};
			visit(k, EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)?;
		}
		Ok(())
	}

	pub fn range_bytes_visit(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		for (k, row) in extern_c::range(
			self.ctx,
			start.map(GroupStateKey::as_encoded),
			end.map(GroupStateKey::as_encoded),
		)? {
			let Some(k) = GroupStateKey::from_framed(k) else {
				continue;
			};
			visit(k, EncodedOperatorRow::try_from(row).map_err(ValueError::from)?)?;
		}
		Ok(())
	}

	#[inline]
	fn written_at(&self) -> DateTime {
		// SAFETY: ExternCOperatorContext::new asserts ctx.ctx is non-null, and the host keeps the
		// ExternCContext alive and aligned for at least the lifetime of the borrow this State was created
		// from.
		DateTime::from_nanos(unsafe { (*self.ctx.ctx).written_at_nanos })
	}
}

#[inline]
pub fn encode_payload<T: OperatorState>(value: &T, now: DateTime) -> Result<EncodedOperatorRow> {
	Ok(value.encode_state(now).map_err(ValueError::from)?)
}

#[inline]
pub fn decode_payload<T: OperatorState>(row: &EncodedOperatorRow) -> Result<T> {
	Ok(decode(row).map_err(ValueError::from)?)
}

pub trait RawStatefulOperator {
	fn state_get<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &GroupStateKey,
	) -> Result<Option<T>> {
		ctx.state().get(key)
	}

	fn state_set<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &GroupStateKey,
		value: &T,
	) -> Result<()> {
		ctx.state().set(key, value)
	}

	fn state_remove(&self, ctx: &mut impl OperatorContext, key: &GroupStateKey) -> Result<()> {
		ctx.state().remove(key)
	}

	fn state_clear(&self, ctx: &mut impl OperatorContext) -> Result<()> {
		ctx.state().clear()
	}
}
