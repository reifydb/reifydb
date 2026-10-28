// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackDictionaryChangeOperations, dictionary::Dictionary, id::NamespaceId,
};
use reifydb_value::{Result, value::dictionary::DictionaryId};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalDictionaryChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackDictionaryChangeOperations for AdminTransaction {
	fn track_dictionary_created(&mut self, dictionary: Dictionary) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(dictionary),
			op: Create,
		};
		self.changes.add_dictionary_change(change);
		Ok(())
	}

	fn track_dictionary_deleted(&mut self, dictionary: Dictionary) -> Result<()> {
		let change = Change {
			pre: Some(dictionary),
			post: None,
			op: Delete,
		};
		self.changes.add_dictionary_change(change);
		Ok(())
	}
}

impl TransactionalDictionaryChanges for AdminTransaction {
	fn find_dictionary(&self, id: DictionaryId) -> Option<&Dictionary> {
		for change in self.changes.dictionary.iter().rev() {
			if let Some(dictionary) = &change.post {
				if dictionary.id == id {
					return Some(dictionary);
				}
			} else if let Some(dictionary) = &change.pre
				&& dictionary.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_dictionary_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Dictionary> {
		for change in self.changes.dictionary.iter().rev() {
			if let Some(dictionary) = &change.post {
				if dictionary.namespace == namespace && dictionary.name == name {
					return Some(dictionary);
				}
			} else if let Some(dictionary) = &change.pre
				&& dictionary.namespace == namespace
				&& dictionary.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_dictionary_deleted(&self, id: DictionaryId) -> bool {
		self.changes
			.dictionary
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|d| d.id) == Some(id))
	}

	fn is_dictionary_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.dictionary.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|d| d.namespace == namespace && d.name == name)
					.unwrap_or(false)
		})
	}
}
