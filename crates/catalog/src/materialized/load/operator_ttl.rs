// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	operator_ttl::{OperatorTtlKey, OperatorTtlKeyRange},
};
use reifydb_transaction::transaction::Transaction;
use tracing::warn;

use super::MaterializedCatalog;
use crate::{Result, store::ttl::decode_ttl_config};

pub(crate) fn load_operator_ttls(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = OperatorTtlKeyRange::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		let Some(key) = OperatorTtlKey::decode(&multi.key) else {
			warn!("Failed to decode OperatorTtlKey from catalog entry, skipping");
			continue;
		};
		let Some(config) = decode_ttl_config(&multi.row) else {
			warn!(?key.node, "Failed to decode operator TTL config from catalog entry, skipping");
			continue;
		};
		catalog.set_operator_ttl(key.node, version, Some(config));
	}

	Ok(())
}
