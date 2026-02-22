// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, sumtype::SumTypeDef},
	key::{namespace_sumtype::NamespaceSumTypeKey, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;

use super::schema::{sumtype as sumtype_schema, sumtype_namespace};
use crate::{
	CatalogStore,
	error::{CatalogError, CatalogObjectKind},
	store::sequence::system::SystemSequence,
};

#[derive(Debug, Clone)]
pub struct SumTypeToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub def: SumTypeDef,
}

impl CatalogStore {
	pub(crate) fn create_sumtype(
		txn: &mut AdminTransaction,
		to_create: SumTypeToCreate,
	) -> crate::Result<SumTypeDef> {
		let namespace_id = to_create.namespace;

		if let Some(_existing) = CatalogStore::find_sumtype_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::SumType,
				namespace: namespace.name,
				name: to_create.name.text().to_string(),
				fragment: to_create.name.clone(),
			}
			.into());
		}

		let sumtype_id = SystemSequence::next_sumtype_id(txn)?;

		let variants_json =
			serde_json::to_string(&to_create.def.variants).expect("failed to serialize variants");

		let mut row = sumtype_schema::SCHEMA.allocate();
		sumtype_schema::SCHEMA.set_u64(&mut row, sumtype_schema::ID, sumtype_id);
		sumtype_schema::SCHEMA.set_u64(&mut row, sumtype_schema::NAMESPACE, namespace_id);
		sumtype_schema::SCHEMA.set_utf8(&mut row, sumtype_schema::NAME, to_create.name.text());
		sumtype_schema::SCHEMA.set_utf8(&mut row, sumtype_schema::VARIANTS_JSON, &variants_json);

		txn.set(&SumTypeKey::encoded(sumtype_id), row)?;

		let mut ns_row = sumtype_namespace::SCHEMA.allocate();
		sumtype_namespace::SCHEMA.set_u64(&mut ns_row, sumtype_namespace::ID, sumtype_id);
		sumtype_namespace::SCHEMA.set_utf8(&mut ns_row, sumtype_namespace::NAME, to_create.name.text());

		txn.set(&NamespaceSumTypeKey::encoded(namespace_id, sumtype_id), ns_row)?;

		Ok(CatalogStore::get_sumtype(&mut Transaction::Admin(&mut *txn), sumtype_id)?)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{interface::catalog::sumtype::VariantDef, key::namespace_sumtype::NamespaceSumTypeKey};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::{fragment::Fragment, value::sumtype::SumTypeId};

	use super::*;
	use crate::{CatalogStore, store::sumtype::schema::sumtype_namespace, test_utils::ensure_test_namespace};

	#[test]
	fn test_create_sumtype() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let variants = vec![
			VariantDef {
				tag: 0,
				name: "Active".to_string(),
				fields: vec![],
			},
			VariantDef {
				tag: 1,
				name: "Inactive".to_string(),
				fields: vec![],
			},
		];

		let to_create = SumTypeToCreate {
			name: Fragment::internal("Status"),
			namespace: test_namespace.id,
			def: SumTypeDef {
				id: SumTypeId(0),
				namespace: test_namespace.id,
				name: "Status".to_string(),
				variants: variants.clone(),
			},
		};

		let result = CatalogStore::create_sumtype(&mut txn, to_create).unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "Status");
		assert_eq!(result.variants, variants);
	}

	#[test]
	fn test_create_sumtype_duplicate_error() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SumTypeToCreate {
			name: Fragment::internal("Direction"),
			namespace: test_namespace.id,
			def: SumTypeDef {
				id: SumTypeId(0),
				namespace: test_namespace.id,
				name: "Direction".to_string(),
				variants: vec![VariantDef {
					tag: 0,
					name: "Up".to_string(),
					fields: vec![],
				}],
			},
		};

		let result = CatalogStore::create_sumtype(&mut txn, to_create.clone()).unwrap();
		assert!(result.id.0 > 0);

		let err = CatalogStore::create_sumtype(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_sumtype_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create1 = SumTypeToCreate {
			name: Fragment::internal("Color"),
			namespace: test_namespace.id,
			def: SumTypeDef {
				id: SumTypeId(0),
				namespace: test_namespace.id,
				name: "Color".to_string(),
				variants: vec![],
			},
		};
		CatalogStore::create_sumtype(&mut txn, to_create1).unwrap();

		let to_create2 = SumTypeToCreate {
			name: Fragment::internal("Shape"),
			namespace: test_namespace.id,
			def: SumTypeDef {
				id: SumTypeId(0),
				namespace: test_namespace.id,
				name: "Shape".to_string(),
				variants: vec![],
			},
		};
		CatalogStore::create_sumtype(&mut txn, to_create2).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceSumTypeKey::full_scan(test_namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let link = &links[0];
		let row = &link.values;
		let id2 = sumtype_namespace::SCHEMA.get_u64(row, sumtype_namespace::ID);
		assert!(id2 > 0);
		assert_eq!(sumtype_namespace::SCHEMA.get_utf8(row, sumtype_namespace::NAME), "Shape");

		let link = &links[1];
		let row = &link.values;
		let id1 = sumtype_namespace::SCHEMA.get_u64(row, sumtype_namespace::ID);
		assert!(id2 > id1);
		assert_eq!(sumtype_namespace::SCHEMA.get_utf8(row, sumtype_namespace::NAME), "Color");
	}
}
