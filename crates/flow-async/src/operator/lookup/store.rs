// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb_codec::row::{
	bytes::EncodedBytes,
	operator::state::{decode, encode},
	pod::EncodedPodRow,
};
use reifydb_core::{
	internal_err,
	key::{
		operator::{
			keyspace::lookup::{LookupRead, LookupReadKey},
			state::{GroupId, GroupStateKey},
		},
		typed::direction::Asc,
	},
	state::{batch::StateBatch, typed::typed_key},
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{error::FlowStateError, operator::host::HostContext};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredRead {
	pub(crate) output: RowNumber,
	pub(crate) right: Option<(RowNumber, Vec<u8>)>,
}

impl StoredRead {
	pub(crate) fn new(output: RowNumber, right: Option<&(RowNumber, EncodedBytes)>) -> Self {
		Self {
			output,
			right: right.map(|(number, bytes)| (*number, bytes.as_ref().to_vec())),
		}
	}

	pub(crate) fn right(&self) -> Option<(RowNumber, EncodedBytes)> {
		self.right.as_ref().map(|(number, bytes)| (*number, EncodedBytes(CowVec::new(bytes.clone()))))
	}
}

pub(crate) fn read_key(row: RowNumber) -> GroupStateKey {
	typed_key::<LookupRead>(
		GroupId::ROOT,
		&LookupReadKey {
			row: Asc(row),
		},
	)
}

pub(crate) struct ReadLedger {
	rows: Vec<RowNumber>,
	initial: HashMap<RowNumber, Option<StoredRead>>,
	current: HashMap<RowNumber, Option<StoredRead>>,
}

impl ReadLedger {
	pub(crate) fn read(host: &mut dyn HostContext, rows: impl IntoIterator<Item = RowNumber>) -> Result<Self> {
		let mut seen = HashSet::new();
		let rows: Vec<RowNumber> = rows.into_iter().filter(|row| seen.insert(*row)).collect();
		let mut initial = HashMap::with_capacity(rows.len());
		if !rows.is_empty() {
			let batch = host.state_batch(rows.iter().map(|&row| read_key(row)).collect())?;
			for (slot, &row) in rows.iter().enumerate() {
				initial.insert(row, batch.value(slot).map(decode_read).transpose()?);
			}
		}
		Ok(Self {
			rows,
			current: initial.clone(),
			initial,
		})
	}

	pub(crate) fn take(&mut self, row: RowNumber) -> Result<Option<StoredRead>> {
		Ok(self.slot(row)?.take())
	}

	pub(crate) fn store(&mut self, row: RowNumber, read: StoredRead) -> Result<()> {
		*self.slot(row)? = Some(read);
		Ok(())
	}

	fn slot(&mut self, row: RowNumber) -> Result<&mut Option<StoredRead>> {
		match self.current.get_mut(&row) {
			Some(slot) => Ok(slot),
			None => internal_err!("lookup row {} changed its read without reading it first", row.value()),
		}
	}

	pub(crate) fn commit(self, host: &mut dyn HostContext) -> Result<()> {
		if self.rows.is_empty() {
			return Ok(());
		}
		let mut keys = Vec::with_capacity(self.rows.len());
		let mut befores = Vec::with_capacity(self.rows.len());
		let mut afters = Vec::with_capacity(self.rows.len());
		for row in &self.rows {
			keys.push(read_key(*row));
			befores.push(self.initial[row].as_ref().map(encode_read).transpose()?);
			afters.push(self.current[row].as_ref().map(encode_read).transpose()?);
		}
		let mut batch = StateBatch::from_parts(keys, befores, vec![None; self.rows.len()])?;
		for (slot, after) in afters.into_iter().enumerate() {
			match after {
				Some(row) => batch.set(slot, row),
				None => batch.remove(slot),
			}
		}
		host.state_write_batch(batch)
	}
}

fn encode_read(read: &StoredRead) -> Result<EncodedPodRow> {
	encode(read).map_err(|e| {
		FlowStateError::Encode {
			state: "LookupRead",
			cause: e.to_string(),
		}
		.into()
	})
}

pub(crate) fn decode_read(stored: &EncodedPodRow) -> Result<StoredRead> {
	decode(stored).map_err(|e| {
		FlowStateError::Decode {
			state: "LookupRead",
			cause: e.to_string(),
		}
		.into()
	})
}
