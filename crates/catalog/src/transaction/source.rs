// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	NamespaceId, QueryTransaction, SourceDef, SourceId,
};
use reifydb_type::IntoFragment;

pub trait CatalogSourceQueryOperations {
	fn find_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		source: impl IntoFragment<'a>,
	) -> crate::Result<Option<SourceDef>>;

	fn find_source(
		&mut self,
		id: SourceId,
	) -> crate::Result<Option<SourceDef>>;

	fn get_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<SourceDef>;
}

impl<T: QueryTransaction> CatalogSourceQueryOperations for T {
	fn find_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		source: impl IntoFragment<'a>,
	) -> reifydb_core::Result<Option<SourceDef>> {
		todo!()
	}

	fn find_source(
		&mut self,
		id: SourceId,
	) -> reifydb_core::Result<Option<SourceDef>> {
		todo!()
	}

	fn get_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<SourceDef> {
		todo!()
	}
}
