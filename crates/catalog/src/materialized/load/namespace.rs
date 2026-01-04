// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::StreamExt;
use reifydb_core::interface::NamespaceKey;
use reifydb_transaction::IntoStandardTransaction;

use crate::{MaterializedCatalog, store::namespace};

/// Load all namespaces from storage
pub(crate) async fn load_namespaces(
	rx: &mut impl IntoStandardTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = NamespaceKey::full_scan();
	let mut stream = txn.range(range, 1024)?;

	while let Some(entry) = stream.next().await {
		let multi = entry?;
		let version = multi.version;
		let namespace_def = namespace::convert_namespace(multi);
		catalog.set_namespace(namespace_def.id, version, Some(namespace_def));
	}

	Ok(())
}
