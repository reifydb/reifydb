// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb_codec::row::{
	operator::state::{OperatorState, decode},
	pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	internal_err,
	key::{
		operator::{
			keyspace::lookup::{LookupRead, LookupReadByVersion, LookupReadByVersionKey, LookupReadKey},
			state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, keyspace_inner_range},
		},
		typed::direction::Asc,
	},
	state::{
		batch::StateBatch,
		typed::{SuffixBytes, typed_key},
	},
};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::operator::host::HostContext;

pub(crate) fn read_key(row: RowNumber) -> GroupStateKey {
	typed_key::<LookupRead>(
		GroupId::ROOT,
		&LookupReadKey {
			row: Asc(row),
		},
	)
}

pub(crate) fn read_by_version_key(version: CommitVersion, row: RowNumber) -> GroupStateKey {
	typed_key::<LookupReadByVersion>(
		GroupId::ROOT,
		&LookupReadByVersionKey {
			version: Asc(version.0),
			row: Asc(row),
		},
	)
}

pub(crate) struct ReadLedger {
	rows: Vec<RowNumber>,
	initial: HashMap<RowNumber, Option<(CommitVersion, RowNumber)>>,
	current: HashMap<RowNumber, Option<(CommitVersion, RowNumber)>>,
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

	pub(crate) fn held(&self, row: RowNumber) -> Option<CommitVersion> {
		self.initial.get(&row).copied().flatten().map(|(version, _)| version)
	}

	pub(crate) fn take(&mut self, row: RowNumber) -> Result<Option<(CommitVersion, RowNumber)>> {
		Ok(self.slot(row)?.take())
	}

	pub(crate) fn store(&mut self, row: RowNumber, version: CommitVersion, output: RowNumber) -> Result<()> {
		*self.slot(row)? = Some((version, output));
		Ok(())
	}

	fn slot(&mut self, row: RowNumber) -> Result<&mut Option<(CommitVersion, RowNumber)>> {
		match self.current.get_mut(&row) {
			Some(slot) => Ok(slot),
			None => internal_err!("lookup row {} changed its read without reading it first", row.value()),
		}
	}

	pub(crate) fn commit(self, host: &mut dyn HostContext) -> Result<()> {
		if self.rows.is_empty() {
			return Ok(());
		}
		let mut writes: Vec<(GroupStateKey, Option<EncodedPodRow>, Option<EncodedPodRow>)> = Vec::new();
		for row in &self.rows {
			let before = self.initial[row];
			let after = self.current[row];
			writes.push((
				read_key(*row),
				before.map(encode_read).transpose()?,
				after.map(encode_read).transpose()?,
			));
			let held = before.map(|(version, _)| version);
			let kept = after.map(|(version, _)| version);
			if let Some(version) = held {
				let stays = (kept == Some(version)).then(marker);
				writes.push((read_by_version_key(version, *row), Some(marker()), stays));
			}
			if let Some(version) = kept.filter(|version| held != Some(*version)) {
				writes.push((read_by_version_key(version, *row), None, Some(marker())));
			}
		}
		let (keys, values): (Vec<_>, Vec<_>) =
			writes.iter().map(|(key, before, _)| (key.clone(), before.clone())).unzip();
		let mut batch = StateBatch::from_parts(keys, values, vec![None; writes.len()])?;
		for (slot, (_, _, after)) in writes.into_iter().enumerate() {
			match after {
				Some(row) => batch.set(slot, row),
				None => batch.remove(slot),
			}
		}
		host.state_write_batch(batch)
	}
}

fn marker() -> EncodedPodRow {
	EncodedPodRow::new(&[])
}

fn encode_read((version, output): (CommitVersion, RowNumber)) -> Result<EncodedPodRow> {
	Ok((version.0, output.0).encode_state()?)
}

pub(crate) fn decode_read(stored: &EncodedPodRow) -> Result<(CommitVersion, RowNumber)> {
	let (version, output) = decode::<(u64, u64)>(stored)?;
	Ok((CommitVersion(version), RowNumber(output)))
}

pub(crate) fn oldest_read(host: &mut dyn HostContext) -> Result<Option<CommitVersion>> {
	let first = host.state_range_limited(
		keyspace_inner_range(GroupId::ROOT, KeyspaceId::LOOKUP_READ_BY_VERSION),
		Some(1),
	)?;
	let Some((key, _)) = first.into_iter().next() else {
		return Ok(None);
	};
	let Some(suffix) = OperatorStateKey::decode_inner(key.as_slice())
		.and_then(|(_, _, suffix)| LookupReadByVersionKey::from_suffix_bytes(suffix))
	else {
		return internal_err!("lookup read-by-version key {:?} does not decode", key.as_slice());
	};
	Ok(Some(CommitVersion(suffix.version.0)))
}
