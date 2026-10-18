// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::key::column::ColumnBlockKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{Result, store::column_block::find::decode_column_block};

pub(crate) fn load_column_blocks(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = ColumnBlockKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let block = decode_column_block(EncodedCatalogRow::view(&multi.bytes));
		catalog.set_column_block(block.id, version, Some(block));
	}

	Ok(())
}
