// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_core::interface::catalog::{
	change::{CatalogTrackIdentityAttributeChangeOperations, CatalogTrackIdentityAttributeValueChangeOperations},
	identity::{IdentityAttribute, IdentityAttributeId, IdentityAttributeValue},
};
use reifydb_transaction::{
	change::{TransactionalIdentityAttributeChanges, TransactionalIdentityAttributeValueChanges},
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::value::{identity::IdentityId, value_type::ValueType};
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::identity_attribute::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_identity_attribute_by_name(
		&self,
		txn: &mut Transaction<'_>,
		name: &str,
	) -> Result<Option<IdentityAttribute>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				if let Some(attribute) =
					TransactionalIdentityAttributeChanges::find_identity_attribute_by_name(
						admin, name,
					) {
					return Ok(Some(attribute.clone()));
				}

				if TransactionalIdentityAttributeChanges::is_identity_attribute_deleted_by_name(
					admin, name,
				) {
					return Ok(None);
				}

				if let Some(attribute) =
					self.cache.find_identity_attribute_by_name_at(name, admin.version())
				{
					return Ok(Some(attribute));
				}

				if let Some(attribute) = CatalogStore::find_identity_attribute_by_name(
					&mut Transaction::Admin(&mut *admin),
					name,
				)? {
					warn!("User attribute '{}' found in storage but not in CatalogCache", name);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(attribute) =
					self.cache.find_identity_attribute_by_name_at(name, cmd.version())
				{
					return Ok(Some(attribute));
				}

				if let Some(attribute) = CatalogStore::find_identity_attribute_by_name(
					&mut Transaction::Command(&mut *cmd),
					name,
				)? {
					warn!("User attribute '{}' found in storage but not in CatalogCache", name);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(attribute) =
					self.cache.find_identity_attribute_by_name_at(name, qry.version())
				{
					return Ok(Some(attribute));
				}

				if let Some(attribute) = CatalogStore::find_identity_attribute_by_name(
					&mut Transaction::Query(&mut *qry),
					name,
				)? {
					warn!("User attribute '{}' found in storage but not in CatalogCache", name);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(attribute) =
					TransactionalIdentityAttributeChanges::find_identity_attribute_by_name(
						t.inner, name,
					) {
					return Ok(Some(attribute.clone()));
				}

				if TransactionalIdentityAttributeChanges::is_identity_attribute_deleted_by_name(
					t.inner, name,
				) {
					return Ok(None);
				}

				if let Some(attribute) =
					self.cache.find_identity_attribute_by_name_at(name, t.inner.version())
				{
					return Ok(Some(attribute));
				}

				if let Some(attribute) = CatalogStore::find_identity_attribute_by_name(
					&mut Transaction::Admin(&mut *t.inner),
					name,
				)? {
					warn!("User attribute '{}' found in storage but not in CatalogCache", name);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(attribute) =
					self.cache.find_identity_attribute_by_name_at(name, rep.version())
				{
					return Ok(Some(attribute));
				}

				if let Some(attribute) = CatalogStore::find_identity_attribute_by_name(
					&mut Transaction::Replica(&mut *rep),
					name,
				)? {
					warn!("User attribute '{}' found in storage but not in CatalogCache", name);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::identity_attribute::find", level = "trace", skip(self, txn))]
	pub fn find_identity_attribute(
		&self,
		txn: &mut Transaction<'_>,
		id: IdentityAttributeId,
	) -> Result<Option<IdentityAttribute>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				if let Some(attribute) =
					TransactionalIdentityAttributeChanges::find_identity_attribute(admin, id)
				{
					return Ok(Some(attribute.clone()));
				}

				if TransactionalIdentityAttributeChanges::is_identity_attribute_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(attribute) = self.cache.find_identity_attribute_at(id, admin.version()) {
					return Ok(Some(attribute));
				}

				if let Some(attribute) =
					CatalogStore::find_identity_attribute(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!("User attribute '{}' found in storage but not in CatalogCache", id);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(attribute) =
					TransactionalIdentityAttributeChanges::find_identity_attribute(t.inner, id)
				{
					return Ok(Some(attribute.clone()));
				}

				if TransactionalIdentityAttributeChanges::is_identity_attribute_deleted(t.inner, id) {
					return Ok(None);
				}

				if let Some(attribute) = self.cache.find_identity_attribute_at(id, t.inner.version()) {
					return Ok(Some(attribute));
				}

				if let Some(attribute) = CatalogStore::find_identity_attribute(
					&mut Transaction::Admin(&mut *t.inner),
					id,
				)? {
					warn!("User attribute '{}' found in storage but not in CatalogCache", id);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
			_ => {
				let version = match txn.reborrow() {
					Transaction::Command(cmd) => cmd.version(),
					Transaction::Query(qry) => qry.version(),
					Transaction::Replica(rep) => rep.version(),
					_ => unreachable!(),
				};

				if let Some(attribute) = self.cache.find_identity_attribute_at(id, version) {
					return Ok(Some(attribute));
				}

				if let Some(attribute) = CatalogStore::find_identity_attribute(txn, id)? {
					warn!("User attribute '{}' found in storage but not in CatalogCache", id);
					return Ok(Some(attribute));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::identity_attribute::list_all", level = "trace", skip(self, txn))]
	pub fn list_identity_attributes(&self, txn: &mut Transaction<'_>) -> Result<Vec<IdentityAttribute>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self.cache.list_all_identity_attributes_at(cmd.version())),
			Transaction::Admin(admin) => {
				let mut attributes = self.cache.list_all_identity_attributes_at(admin.version());
				for change in &admin.changes.identity_attribute {
					if let Some(attribute) = &change.post
						&& !attributes.iter().any(|existing| existing.id == attribute.id)
					{
						attributes.push(attribute.clone());
					}
				}
				attributes.retain(|a| !admin.is_identity_attribute_deleted(a.id));
				Ok(attributes)
			}
			Transaction::Query(qry) => Ok(self.cache.list_all_identity_attributes_at(qry.version())),
			Transaction::Test(t) => {
				let mut attributes = self.cache.list_all_identity_attributes_at(t.inner.version());
				for change in &t.inner.changes.identity_attribute {
					if let Some(attribute) = &change.post
						&& !attributes.iter().any(|existing| existing.id == attribute.id)
					{
						attributes.push(attribute.clone());
					}
				}
				attributes.retain(|a| !t.inner.is_identity_attribute_deleted(a.id));
				Ok(attributes)
			}
			Transaction::Replica(rep) => Ok(self.cache.list_all_identity_attributes_at(rep.version())),
		}
	}

	#[instrument(name = "catalog::identity_attribute::create", level = "info", skip(self, txn))]
	pub fn create_identity_attribute(
		&self,
		txn: &mut AdminTransaction,
		name: &str,
		value_type: ValueType,
	) -> Result<IdentityAttribute> {
		let attribute = CatalogStore::create_identity_attribute(txn, name, value_type)?;
		txn.track_identity_attribute_created(attribute.clone())?;
		Ok(attribute)
	}

	#[instrument(name = "catalog::identity_attribute::drop", level = "info", skip(self, txn))]
	pub fn drop_identity_attribute(&self, txn: &mut AdminTransaction, id: IdentityAttributeId) -> Result<()> {
		let values = CatalogStore::find_identity_attribute_values_for_attribute(
			&mut Transaction::Admin(&mut *txn),
			id,
		)?;
		if let Some(attribute) = CatalogStore::find_identity_attribute(&mut Transaction::Admin(&mut *txn), id)?
		{
			CatalogStore::drop_identity_attribute(txn, id)?;
			for value in values {
				txn.track_identity_attribute_value_deleted(value)?;
			}
			txn.track_identity_attribute_deleted(attribute)?;
		} else {
			CatalogStore::drop_identity_attribute(txn, id)?;
			for value in values {
				txn.track_identity_attribute_value_deleted(value)?;
			}
		}
		Ok(())
	}

	#[instrument(name = "catalog::identity_attribute::set_value", level = "debug", skip(self, txn))]
	pub fn set_identity_attribute_value(
		&self,
		txn: &mut AdminTransaction,
		identity: IdentityId,
		attribute: IdentityAttributeId,
		value: &str,
	) -> Result<IdentityAttributeValue> {
		let value = CatalogStore::set_identity_attribute_value(txn, identity, attribute, value)?;
		txn.track_identity_attribute_value_created(value.clone())?;
		Ok(value)
	}

	#[instrument(name = "catalog::identity_attribute::remove_value", level = "debug", skip(self, txn))]
	pub fn remove_identity_attribute_value(
		&self,
		txn: &mut AdminTransaction,
		identity: IdentityId,
		attribute: IdentityAttributeId,
	) -> Result<()> {
		if let Some(value) = CatalogStore::find_identity_attribute_value(
			&mut Transaction::Admin(&mut *txn),
			identity,
			attribute,
		)? {
			CatalogStore::remove_identity_attribute_value(txn, identity, attribute)?;
			txn.track_identity_attribute_value_deleted(value)?;
		} else {
			CatalogStore::remove_identity_attribute_value(txn, identity, attribute)?;
		}
		Ok(())
	}

	#[instrument(name = "catalog::identity_attribute::find_values_for_identity", level = "trace", skip(self, txn))]
	pub fn find_identity_attribute_values(
		&self,
		txn: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<IdentityAttributeValue>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				let version = admin.version();
				let mut values = Vec::new();
				let mut seen = HashSet::new();

				for value in
					TransactionalIdentityAttributeValueChanges::find_identity_attribute_values_for_identity(
						admin, identity,
					) {
					seen.insert(value.attribute);
					values.push(value.clone());
				}

				for value in self.cache.find_identity_attribute_values_at(identity, version) {
					if !seen.contains(&value.attribute)
						&& !TransactionalIdentityAttributeValueChanges::is_identity_attribute_value_deleted(
							admin,
							identity,
							value.attribute,
						) {
						values.push(value);
					}
				}

				Ok(values)
			}
			Transaction::Test(t) => {
				let version = t.inner.version();
				let mut values = Vec::new();
				let mut seen = HashSet::new();

				for value in
					TransactionalIdentityAttributeValueChanges::find_identity_attribute_values_for_identity(
						t.inner, identity,
					) {
					seen.insert(value.attribute);
					values.push(value.clone());
				}

				for value in self.cache.find_identity_attribute_values_at(identity, version) {
					if !seen.contains(&value.attribute)
						&& !TransactionalIdentityAttributeValueChanges::is_identity_attribute_value_deleted(
							t.inner,
							identity,
							value.attribute,
						) {
						values.push(value);
					}
				}

				Ok(values)
			}
			_ => {
				let version = match txn.reborrow() {
					Transaction::Command(cmd) => cmd.version(),
					Transaction::Query(qry) => qry.version(),
					Transaction::Replica(rep) => rep.version(),
					_ => unreachable!(),
				};

				Ok(self.cache.find_identity_attribute_values_at(identity, version))
			}
		}
	}
}
