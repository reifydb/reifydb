// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, OperatorStateKey},
	},
	state::store::{StateStore, TimerKind},
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{timer::Timer, transaction::FlowTransaction};

pub struct OperatorStateStore<'a, T: FlowTransaction> {
	txn: &'a mut T,
	operator: OperatorId,
	now: DateTime,
}

impl<'a, T: FlowTransaction> OperatorStateStore<'a, T> {
	pub fn new(txn: &'a mut T, operator: OperatorId) -> Self {
		let now = txn.written_at();
		Self {
			txn,
			operator,
			now,
		}
	}
}

impl<T: FlowTransaction> StateStore for OperatorStateStore<'_, T> {
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.arm_timer(
			self.operator,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}

	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer(
			self.operator,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(self.txn.flow_watermark())
	}

	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		self.txn.state_get(self.operator, key)
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_get_many(self.operator, keys)?;
		for r in batch.items {
			let Some(decoded) = OperatorStateKey::decode(&r.key) else {
				continue;
			};
			let Some(inner) = GroupStateKey::from_framed(decoded.inner()) else {
				continue;
			};
			visit(inner, EncodedOperatorRow::try_from(r.bytes)?)?;
		}
		Ok(())
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()> {
		self.txn.state_set(self.operator, key, payload)
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.txn.state_remove(self.operator, key)
	}

	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_range(self.operator, range, limit, "operator::store_visit")?;
		for r in batch.items {
			if let Some(decoded) = OperatorStateKey::decode(&r.key)
				&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
			{
				visit(inner, EncodedOperatorRow::try_from(r.bytes)?)?;
			}
		}
		Ok(())
	}

	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
		Ok(self.txn.intern_group(self.operator, group)?.0)
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		self.txn.lookup_group(self.operator, group)
	}

	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		self.txn.get_or_create_row_number(self.operator, group, key)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.operator, group, keys)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.operator, group, key).map(|_| ())
	}

	fn written_at(&self) -> DateTime {
		self.now
	}
}
