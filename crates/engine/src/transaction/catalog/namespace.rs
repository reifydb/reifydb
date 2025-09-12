// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::transaction::CatalogTrackNamespaceChangeOperations;
use reifydb_core::interface::{NamespaceDef, Transaction};

use crate::StandardCommandTransaction;

impl<T: Transaction> CatalogTrackNamespaceChangeOperations
	for StandardCommandTransaction<T>
{
	fn track_namespace_def_created(
		&mut self,
		namespace: NamespaceDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}

	fn track_namespace_def_updated(
		&mut self,
		pre: NamespaceDef,
		post: NamespaceDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}

	fn track_namespace_def_deleted(
		&mut self,
		namespace: NamespaceDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}
}
