// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	state::typed::{SuffixBytes, typed_key},
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

pub(crate) fn stored_read(host: &mut dyn HostContext, row: RowNumber) -> Result<Option<(CommitVersion, RowNumber)>> {
	match host.state_get(&read_key(row))? {
		Some(stored) => {
			let (version, output) = decode::<(u64, u64)>(&stored)?;
			Ok(Some((CommitVersion(version), RowNumber(output))))
		}
		None => Ok(None),
	}
}

pub(crate) fn store_read(
	host: &mut dyn HostContext,
	row: RowNumber,
	version: CommitVersion,
	output: RowNumber,
) -> Result<()> {
	if let Some((previous, _)) = stored_read(host, row)? {
		host.state_remove(&read_by_version_key(previous, row))?;
	}
	host.state_set(&read_key(row), (version.0, output.0).encode_state()?)?;
	host.state_set(&read_by_version_key(version, row), EncodedPodRow::new(&[]))
}

pub(crate) fn take_read(host: &mut dyn HostContext, row: RowNumber) -> Result<Option<(CommitVersion, RowNumber)>> {
	let Some((version, output)) = stored_read(host, row)? else {
		return Ok(None);
	};
	host.state_remove(&read_key(row))?;
	host.state_remove(&read_by_version_key(version, row))?;
	Ok(Some((version, output)))
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
