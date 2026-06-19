// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::column_snapshot::ColumnSnapshotKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{Result, store::column_snapshot::find::decode_column_snapshot};

pub(crate) fn load_column_snapshots(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = ColumnSnapshotKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let snapshot = decode_column_snapshot(&multi.row);
		catalog.set_column_snapshot(snapshot.id, version, Some(snapshot));
	}

	Ok(())
}
