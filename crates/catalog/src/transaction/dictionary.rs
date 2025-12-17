// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, DictionaryDef, DictionaryId, NamespaceId, QueryTransaction, TransactionalChanges,
	TransactionalDictionaryChanges, interceptor::WithInterceptors,
};
use reifydb_type::{
	IntoFragment,
	diagnostic::catalog::{dictionary_already_exists, dictionary_not_found},
	error, internal, return_error,
};
use tracing::{instrument, warn};

use crate::{
	CatalogNamespaceQueryOperations, CatalogStore, store::dictionary::create::DictionaryToCreate,
	transaction::MaterializedCatalogTransaction,
};

pub trait CatalogDictionaryCommandOperations {
	fn create_dictionary(&mut self, to_create: DictionaryToCreate) -> crate::Result<DictionaryDef>;
}

pub trait CatalogTrackDictionaryChangeOperations {
	fn track_dictionary_def_created(&mut self, dictionary: DictionaryDef) -> crate::Result<()>;

	fn track_dictionary_def_updated(&mut self, pre: DictionaryDef, post: DictionaryDef) -> crate::Result<()>;

	fn track_dictionary_def_deleted(&mut self, dictionary: DictionaryDef) -> crate::Result<()>;
}

pub trait CatalogDictionaryQueryOperations: CatalogNamespaceQueryOperations {
	fn find_dictionary(&mut self, id: DictionaryId) -> crate::Result<Option<DictionaryDef>>;

	fn find_dictionary_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<DictionaryDef>>;

	fn get_dictionary(&mut self, id: DictionaryId) -> crate::Result<DictionaryDef>;

	fn get_dictionary_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<DictionaryDef>;
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackDictionaryChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogDictionaryCommandOperations for CT
{
	#[instrument(name = "catalog::dictionary::create", level = "debug", skip(self, to_create))]
	fn create_dictionary(&mut self, to_create: DictionaryToCreate) -> reifydb_core::Result<DictionaryDef> {
		if let Some(dictionary) = self.find_dictionary_by_name(to_create.namespace, &to_create.dictionary)? {
			let namespace = self.get_namespace(to_create.namespace)?;
			return_error!(dictionary_already_exists(to_create.fragment, &namespace.name, &dictionary.name));
		}
		let result = CatalogStore::create_dictionary(self, to_create)?;
		self.track_dictionary_def_created(result.clone())?;
		Ok(result)
	}
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges> CatalogDictionaryQueryOperations
	for QT
{
	#[instrument(name = "catalog::dictionary::find", level = "trace", skip(self))]
	fn find_dictionary(&mut self, id: DictionaryId) -> reifydb_core::Result<Option<DictionaryDef>> {
		// 1. Check transactional changes first
		// nop for QueryTransaction
		if let Some(dictionary) = TransactionalDictionaryChanges::find_dictionary(self, id) {
			return Ok(Some(dictionary.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalDictionaryChanges::is_dictionary_deleted(self, id) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(dictionary) = self.catalog().find_dictionary(id, self.version()) {
			return Ok(Some(dictionary));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(dictionary) = CatalogStore::find_dictionary(self, id)? {
			warn!("Dictionary with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(dictionary));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::dictionary::find_by_name", level = "trace", skip(self, name))]
	fn find_dictionary_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<Option<DictionaryDef>> {
		let name = name.into_fragment();

		// 1. Check transactional changes first
		// nop for QueryTransaction
		if let Some(dictionary) =
			TransactionalDictionaryChanges::find_dictionary_by_name(self, namespace, name.as_borrowed())
		{
			return Ok(Some(dictionary.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalDictionaryChanges::is_dictionary_deleted_by_name(self, namespace, name.as_borrowed()) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(dictionary) = self.catalog().find_dictionary_by_name(namespace, name.text(), self.version())
		{
			return Ok(Some(dictionary));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(dictionary) = CatalogStore::find_dictionary_by_name(self, namespace, name.text())? {
			warn!(
				"Dictionary '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
				name.text(),
				namespace
			);
			return Ok(Some(dictionary));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::dictionary::get", level = "trace", skip(self))]
	fn get_dictionary(&mut self, id: DictionaryId) -> reifydb_core::Result<DictionaryDef> {
		self.find_dictionary(id)?.ok_or_else(|| {
			error!(internal!(
				"Dictionary with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::dictionary::get_by_name", level = "trace", skip(self, name))]
	fn get_dictionary_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<DictionaryDef> {
		let name = name.into_fragment();

		// Try to get the namespace name for the error message
		let namespace_name = self
			.find_namespace(namespace)?
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		self.find_dictionary_by_name(namespace, name.as_borrowed())?
			.ok_or_else(|| error!(dictionary_not_found(name.as_borrowed(), &namespace_name, name.text())))
	}
}
