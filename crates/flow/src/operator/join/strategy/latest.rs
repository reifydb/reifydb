// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::value::column::columns::Columns;
use reifydb_value::{Result, util::hash::Hash128, value::row_number::RowNumber};
use tracing::instrument;

use super::hash::{build_shape, columns_from_block, encode_row};
use crate::operator::{
	host::HostContext,
	join::store::{Store, body_bytes},
};

#[instrument(name = "flow::operator::join::latest::overwrite_right_slot", level = "trace", skip_all, fields(rows = indices.len()))]
pub(crate) fn overwrite_right_slot(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> Result<Option<Columns>> {
	if indices.is_empty() {
		return Ok(None);
	}
	let shape = build_shape(columns);
	right.set_row_shape(host, &shape)?;
	let mut stored: Option<EncodedPodRow> = None;
	for &idx in indices {
		let row = encode_row(&shape, columns, idx, host.written_at());
		right.put_row(host, key_hash, RowNumber::MAX, &row)?;
		stored = Some(row);
	}
	match stored {
		Some(row) => Ok(Some(columns_from_block(host, right, vec![(RowNumber::MAX, body_bytes(&row))])?)),
		None => Ok(None),
	}
}

#[instrument(name = "flow::operator::join::latest::read_right_slot", level = "trace", skip_all)]
pub(crate) fn read_right_slot(
	host: &mut dyn HostContext,
	right: &Store,
	key_hash: &Hash128,
) -> Result<Option<Columns>> {
	let Some(group) = right.group_of(host, key_hash)? else {
		return Ok(None);
	};
	Ok(right.slot(host, group)?.map(|(_, columns)| columns))
}

#[instrument(name = "flow::operator::join::latest::remove_right_slot", level = "trace", skip_all)]
pub(crate) fn remove_right_slot(host: &mut dyn HostContext, right: &Store, key_hash: &Hash128) -> Result<()> {
	right.remove_row(host, key_hash, RowNumber::MAX)?;
	Ok(())
}
