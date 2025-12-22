// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperationType::{Create, Update};
use reifydb_catalog::transaction::CatalogTrackDictionaryChangeOperations;
use reifydb_core::interface::{
	Change, DictionaryDef, DictionaryId, NamespaceId, OperationType, OperationType::Delete,
	TransactionalDictionaryChanges,
};
use reifydb_type::Fragment;

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl CatalogTrackDictionaryChangeOperations for StandardCommandTransaction {
	fn track_dictionary_def_created(&mut self, dictionary: DictionaryDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: None,
			post: Some(dictionary),
			op: Create,
		};
		self.changes.add_dictionary_def_change(change);
		Ok(())
	}

	fn track_dictionary_def_updated(
		&mut self,
		pre: DictionaryDef,
		post: DictionaryDef,
	) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_dictionary_def_change(change);
		Ok(())
	}

	fn track_dictionary_def_deleted(&mut self, dictionary: DictionaryDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(dictionary),
			post: None,
			op: Delete,
		};
		self.changes.add_dictionary_def_change(change);
		Ok(())
	}
}

impl TransactionalDictionaryChanges for StandardCommandTransaction {
	fn find_dictionary(&self, id: DictionaryId) -> Option<&DictionaryDef> {
		// Find the last change for this dictionary ID
		for change in self.changes.dictionary_def.iter().rev() {
			if let Some(dictionary) = &change.post {
				if dictionary.id == id {
					return Some(dictionary);
				}
			} else if let Some(dictionary) = &change.pre {
				if dictionary.id == id && change.op == Delete {
					// Dictionary was deleted
					return None;
				}
			}
		}
		None
	}

	fn find_dictionary_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&DictionaryDef> {
		self.changes
			.dictionary_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|d| d.namespace == namespace && d.name == name))
	}

	fn is_dictionary_deleted(&self, id: DictionaryId) -> bool {
		self.changes
			.dictionary_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|d| d.id) == Some(id))
	}

	fn is_dictionary_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.dictionary_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|d| d.namespace == namespace && d.name == name)
					.unwrap_or(false)
		})
	}
}

impl TransactionalDictionaryChanges for StandardQueryTransaction {
	fn find_dictionary(&self, _id: DictionaryId) -> Option<&DictionaryDef> {
		None
	}

	fn find_dictionary_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&DictionaryDef> {
		None
	}

	fn is_dictionary_deleted(&self, _id: DictionaryId) -> bool {
		false
	}

	fn is_dictionary_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}
