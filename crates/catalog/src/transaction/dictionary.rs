// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::interface::{
	CommandTransaction, DictionaryDef, DictionaryId, NamespaceId, QueryTransaction, TransactionalChanges,
	TransactionalDictionaryChanges, interceptor::WithInterceptors,
};
use reifydb_type::{
	Fragment,
	diagnostic::catalog::{dictionary_already_exists, dictionary_not_found},
	error, internal, return_error,
};
use tracing::{instrument, warn};

use crate::{
	CatalogNamespaceQueryOperations, CatalogStore, store::dictionary::create::DictionaryToCreate,
	transaction::MaterializedCatalogTransaction,
};

#[async_trait]
pub trait CatalogDictionaryCommandOperations: Send {
	async fn create_dictionary(&mut self, to_create: DictionaryToCreate) -> crate::Result<DictionaryDef>;
}

pub trait CatalogTrackDictionaryChangeOperations {
	fn track_dictionary_def_created(&mut self, dictionary: DictionaryDef) -> crate::Result<()>;

	fn track_dictionary_def_updated(&mut self, pre: DictionaryDef, post: DictionaryDef) -> crate::Result<()>;

	fn track_dictionary_def_deleted(&mut self, dictionary: DictionaryDef) -> crate::Result<()>;
}

#[async_trait]
pub trait CatalogDictionaryQueryOperations: CatalogNamespaceQueryOperations + Send {
	async fn find_dictionary(&mut self, id: DictionaryId) -> crate::Result<Option<DictionaryDef>>;

	async fn find_dictionary_by_name(
		&mut self,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<DictionaryDef>>;

	async fn get_dictionary(&mut self, id: DictionaryId) -> crate::Result<DictionaryDef>;

	async fn get_dictionary_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<DictionaryDef>;
}

#[async_trait]
impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackDictionaryChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges
		+ Send
		+ 'static,
> CatalogDictionaryCommandOperations for CT
{
	#[instrument(name = "catalog::dictionary::create", level = "debug", skip(self, to_create))]
	async fn create_dictionary(&mut self, to_create: DictionaryToCreate) -> reifydb_core::Result<DictionaryDef> {
		if let Some(dictionary) =
			self.find_dictionary_by_name(to_create.namespace, to_create.dictionary.as_str()).await?
		{
			let namespace = self.get_namespace(to_create.namespace).await?;
			return_error!(dictionary_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&dictionary.name
			));
		}
		let result = CatalogStore::create_dictionary(self, to_create).await?;
		self.track_dictionary_def_created(result.clone())?;
		Ok(result)
	}
}

#[async_trait]
impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges + Send + 'static>
	CatalogDictionaryQueryOperations for QT
{
	#[instrument(name = "catalog::dictionary::find", level = "trace", skip(self))]
	async fn find_dictionary(&mut self, id: DictionaryId) -> reifydb_core::Result<Option<DictionaryDef>> {
		// 1. Check transactional changes first
		// nop for MultiVersionQueryTransaction
		if let Some(dictionary) = TransactionalDictionaryChanges::find_dictionary(self, id) {
			return Ok(Some(dictionary.clone()));
		}

		// 2. Check if deleted
		// nop for MultiVersionQueryTransaction
		if TransactionalDictionaryChanges::is_dictionary_deleted(self, id) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(dictionary) = self.catalog().find_dictionary(id, self.version()) {
			return Ok(Some(dictionary));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(dictionary) = CatalogStore::find_dictionary(self, id).await? {
			warn!("Dictionary with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(dictionary));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::dictionary::find_by_name", level = "trace", skip(self, name))]
	async fn find_dictionary_by_name(
		&mut self,
		namespace: NamespaceId,
		name: &str,
	) -> reifydb_core::Result<Option<DictionaryDef>> {
		// 1. Check transactional changes first
		// nop for MultiVersionQueryTransaction
		if let Some(dictionary) = TransactionalDictionaryChanges::find_dictionary_by_name(self, namespace, name)
		{
			return Ok(Some(dictionary.clone()));
		}

		// 2. Check if deleted
		// nop for MultiVersionQueryTransaction
		if TransactionalDictionaryChanges::is_dictionary_deleted_by_name(self, namespace, name) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(dictionary) = self.catalog().find_dictionary_by_name(namespace, name, self.version()) {
			return Ok(Some(dictionary));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(dictionary) = CatalogStore::find_dictionary_by_name(self, namespace, name).await? {
			warn!(
				"Dictionary '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
				name, namespace
			);
			return Ok(Some(dictionary));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::dictionary::get", level = "trace", skip(self))]
	async fn get_dictionary(&mut self, id: DictionaryId) -> reifydb_core::Result<DictionaryDef> {
		self.find_dictionary(id).await?.ok_or_else(|| {
			error!(internal!(
				"Dictionary with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::dictionary::get_by_name", level = "trace", skip(self, name))]
	async fn get_dictionary_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> reifydb_core::Result<DictionaryDef> {
		let name = name.into();

		// Try to get the namespace name for the error message
		let namespace_name = self
			.find_namespace(namespace)
			.await?
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		self.find_dictionary_by_name(namespace, name.text())
			.await?
			.ok_or_else(|| error!(dictionary_not_found(name.clone(), &namespace_name, name.text())))
	}
}
