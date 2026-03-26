// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{change::CatalogTrackIdentityChangeOperations, identity::IdentityDef};
use reifydb_type::{Result, value::identity::IdentityId};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalIdentityChanges,
	},
	interceptor::identity_def::{
		IdentityDefPostCreateContext, IdentityDefPostUpdateContext, IdentityDefPreDeleteContext,
		IdentityDefPreUpdateContext,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackIdentityChangeOperations for AdminTransaction {
	fn track_identity_def_created(&mut self, identity: IdentityDef) -> Result<()> {
		self.interceptors.identity_def_post_create.execute(IdentityDefPostCreateContext::new(&identity))?;
		let change = Change {
			pre: None,
			post: Some(identity),
			op: Create,
		};
		self.changes.add_identity_def_change(change);
		Ok(())
	}

	fn track_identity_def_updated(&mut self, pre: IdentityDef, post: IdentityDef) -> Result<()> {
		self.interceptors.identity_def_pre_update.execute(IdentityDefPreUpdateContext::new(&pre))?;
		self.interceptors.identity_def_post_update.execute(IdentityDefPostUpdateContext::new(&pre, &post))?;
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_identity_def_change(change);
		Ok(())
	}

	fn track_identity_def_deleted(&mut self, identity: IdentityDef) -> Result<()> {
		self.interceptors.identity_def_pre_delete.execute(IdentityDefPreDeleteContext::new(&identity))?;
		let change = Change {
			pre: Some(identity),
			post: None,
			op: Delete,
		};
		self.changes.add_identity_def_change(change);
		Ok(())
	}
}

impl TransactionalIdentityChanges for AdminTransaction {
	fn find_identity(&self, id: IdentityId) -> Option<&IdentityDef> {
		for change in self.changes.identity_def.iter().rev() {
			if let Some(identity) = &change.post {
				if identity.id == id {
					return Some(identity);
				}
			} else if let Some(identity) = &change.pre {
				if identity.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_identity_by_name(&self, name: &str) -> Option<&IdentityDef> {
		self.changes
			.identity_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|u| u.name == name))
	}

	fn is_identity_deleted(&self, id: IdentityId) -> bool {
		self.changes
			.identity_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|u| u.id) == Some(id))
	}

	fn is_identity_deleted_by_name(&self, name: &str) -> bool {
		self.changes.identity_def.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|u| u.name == name).unwrap_or(false)
		})
	}
}

impl CatalogTrackIdentityChangeOperations for SubscriptionTransaction {
	fn track_identity_def_created(&mut self, identity: IdentityDef) -> Result<()> {
		self.inner.track_identity_def_created(identity)
	}

	fn track_identity_def_updated(&mut self, pre: IdentityDef, post: IdentityDef) -> Result<()> {
		self.inner.track_identity_def_updated(pre, post)
	}

	fn track_identity_def_deleted(&mut self, identity: IdentityDef) -> Result<()> {
		self.inner.track_identity_def_deleted(identity)
	}
}

impl TransactionalIdentityChanges for SubscriptionTransaction {
	fn find_identity(&self, id: IdentityId) -> Option<&IdentityDef> {
		self.inner.find_identity(id)
	}

	fn find_identity_by_name(&self, name: &str) -> Option<&IdentityDef> {
		self.inner.find_identity_by_name(name)
	}

	fn is_identity_deleted(&self, id: IdentityId) -> bool {
		self.inner.is_identity_deleted(id)
	}

	fn is_identity_deleted_by_name(&self, name: &str) -> bool {
		self.inner.is_identity_deleted_by_name(name)
	}
}
