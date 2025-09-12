// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogTableQueryOperations,
	transaction::CatalogTrackTableChangeOperations,
};
use reifydb_core::interface::{
	NamespaceId, TableDef, TableId, Transaction, VersionedQueryTransaction,
};

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl<T: Transaction> CatalogTrackTableChangeOperations
	for StandardCommandTransaction<T>
{
	fn track_table_def_created(
		&mut self,
		table: TableDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}

	fn track_table_def_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}

	fn track_table_def_deleted(
		&mut self,
		table: TableDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}
}

// impl<T: Transaction> CatalogTableQueryOperations for
// StandardQueryTransaction<T> {     fn find_table_by_name(
//         &mut self,
//         namespace: NamespaceId,
//         name: impl AsRef<str>,
//     ) -> crate::Result<Option<TableDef>> {
//         let name = name.as_ref();
//
//         Ok(self.catalog.find_table_by_name(
//             namespace,
//             name,
//             VersionedQueryTransaction::version(self),
//         ))
//     }
//
//     fn find_table(
//         &mut self,
//         id: TableId,
//     ) -> crate::Result<Option<TableDef>> {
//         Ok(self.catalog.find_table(
//             id,
//             VersionedQueryTransaction::version(self),
//         ))
//     }
//
//     fn get_table_by_name(
//         &mut self,
//         _namespace: NamespaceId,
//         _name: impl AsRef<str>,
//     ) -> reifydb_core::Result<TableDef> {
//         todo!()
//     }
// }
//
