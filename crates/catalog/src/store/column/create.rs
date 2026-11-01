// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{constraint::encode_constraint_info, typeinfo::encode_value_type};
use reifydb_core::{
	interface::catalog::{object::ObjectId, property::ColumnPropertyKind},
	key::column::{ColumnKey, ColumnsKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{
		blob::Blob,
		constraint::{Constraint, TypeConstraint},
		dictionary::DictionaryId,
		value_type::ValueType,
	},
};

fn value_type_blob(ty: &ValueType) -> Blob {
	let mut bytes = Vec::new();
	encode_value_type(ty, &mut bytes).unwrap_or_else(|error| panic!("column type {ty} cannot be encoded: {error}"));
	Blob::from(bytes)
}

fn constraint_blob(type_constraint: &TypeConstraint) -> Blob {
	let mut bytes = Vec::new();
	encode_constraint_info(type_constraint.constraint().as_ref(), &mut bytes).unwrap_or_else(|error| {
		panic!("constraint of column type {} cannot be encoded: {error}", type_constraint.get_type())
	});
	Blob::from(bytes)
}

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::ColumnId,
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::shape::{column, object_column},
		sequence::system::SystemSequence,
	},
};

pub(crate) struct ColumnToCreate {
	pub fragment: Option<Fragment>,
	pub namespace_name: String,
	pub object_name: String, // FIXME refactor to source_name
	pub column: String,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub index: ColumnIndex,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

impl CatalogStore {
	pub(crate) fn create_column(
		txn: &mut AdminTransaction,
		object: impl Into<ObjectId>,
		mut column_to_create: ColumnToCreate,
	) -> Result<Column> {
		let object = object.into();

		Self::reject_existing_column(txn, object, &column_to_create)?;
		column_to_create.constraint = Self::stored_constraint(txn, &column_to_create)?;
		if let Some(ty) = Self::invalid_auto_increment_type(&column_to_create) {
			return Err(CatalogError::AutoIncrementInvalidType {
				column: column_to_create.column.clone(),
				ty,
				fragment: column_to_create.fragment.unwrap_or(Fragment::None),
			}
			.into());
		}

		let id = SystemSequence::next_column_id(txn)?;
		Self::store_column_row(txn, id, object, &column_to_create)?;
		Self::store_object_column_row(txn, id, object, &column_to_create)?;

		Self::create_properties_and_build(txn, id, column_to_create)
	}

	pub(crate) fn create_column_with_id(
		txn: &mut AdminTransaction,
		id: ColumnId,
		object: impl Into<ObjectId>,
		mut column_to_create: ColumnToCreate,
	) -> Result<Column> {
		let object = object.into();

		column_to_create.constraint = Self::stored_constraint(txn, &column_to_create)?;
		Self::store_column_row(txn, id, object, &column_to_create)?;
		Self::store_object_column_row(txn, id, object, &column_to_create)?;

		Self::create_properties_and_build(txn, id, column_to_create)
	}

	fn stored_constraint(txn: &mut AdminTransaction, column_to_create: &ColumnToCreate) -> Result<TypeConstraint> {
		let Some(dict_id) = column_to_create.dictionary_id else {
			return Ok(column_to_create.constraint.clone());
		};
		if u64::from(dict_id) == 0 {
			return Err(CatalogError::InvalidDefinition {
				kind: CatalogObjectKind::Column,
				name: column_to_create.column.clone(),
				field: "dictionary_id",
				reason: "must not be 0".to_string(),
				fragment: column_to_create.fragment.clone().unwrap_or(Fragment::None),
			}
			.into());
		}
		if matches!(column_to_create.constraint.constraint(), Some(Constraint::Dictionary(..))) {
			return Ok(column_to_create.constraint.clone());
		}
		Ok(match Self::find_dictionary(&mut Transaction::Admin(&mut *txn), dict_id)? {
			Some(dictionary) => TypeConstraint::with_constraint(
				column_to_create.constraint.get_type(),
				Constraint::Dictionary(dict_id, dictionary.id_type),
			),
			None => column_to_create.constraint.clone(),
		})
	}

	#[inline]
	fn reject_existing_column(
		txn: &mut AdminTransaction,
		object: ObjectId,
		column_to_create: &ColumnToCreate,
	) -> Result<()> {
		if let Some(column) =
			Self::find_column_by_name(&mut Transaction::Admin(&mut *txn), object, &column_to_create.column)?
		{
			return Err(CatalogError::ColumnAlreadyExists {
				kind: CatalogObjectKind::Table,
				namespace: column_to_create.namespace_name.clone(),
				name: column_to_create.object_name.clone(),
				column: column.name,
				fragment: Fragment::None,
			}
			.into());
		}
		Ok(())
	}

	#[inline]
	fn invalid_auto_increment_type(column_to_create: &ColumnToCreate) -> Option<ValueType> {
		if !column_to_create.auto_increment {
			return None;
		}
		let base_type = column_to_create.constraint.get_type();
		let is_integer_type = matches!(
			base_type,
			ValueType::Int1
				| ValueType::Int2 | ValueType::Int4
				| ValueType::Int8 | ValueType::Int16
				| ValueType::Uint1 | ValueType::Uint2
				| ValueType::Uint4 | ValueType::Uint8
				| ValueType::Uint16
		);
		if is_integer_type {
			None
		} else {
			Some(base_type)
		}
	}

	fn store_column_row(
		txn: &mut AdminTransaction,
		id: ColumnId,
		object: ObjectId,
		column_to_create: &ColumnToCreate,
	) -> Result<()> {
		let mut row = column::allocate();
		column::set_id(&mut row, u64::from(id));
		column::set_object(&mut row, u64::from(object));
		column::set_name(&mut row, &column_to_create.column);
		column::set_value_type(&mut row, &value_type_blob(&column_to_create.constraint.get_type()));
		column::set_index(&mut row, u8::from(column_to_create.index));
		column::set_auto_increment(&mut row, column_to_create.auto_increment);
		column::set_constraint(&mut row, &constraint_blob(&column_to_create.constraint));

		let dict_id_value = column_to_create.dictionary_id.map(u64::from).unwrap_or(0);
		column::set_dictionary_id(&mut row, dict_id_value);

		txn.set(vec![(ColumnsKey::new(id).into(), row.freeze().into())])
	}

	fn store_object_column_row(
		txn: &mut AdminTransaction,
		id: ColumnId,
		object: ObjectId,
		column_to_create: &ColumnToCreate,
	) -> Result<()> {
		let mut row = object_column::allocate();
		object_column::set_id(&mut row, u64::from(id));
		object_column::set_name(&mut row, &column_to_create.column);
		object_column::set_index(&mut row, u8::from(column_to_create.index));
		txn.set(vec![(ColumnKey::new(object, id).into(), row.freeze().into())])
	}

	fn create_properties_and_build(
		txn: &mut AdminTransaction,
		id: ColumnId,
		column_to_create: ColumnToCreate,
	) -> Result<Column> {
		let properties = column_to_create
			.properties
			.into_iter()
			.map(|property| Self::create_column_property(txn, id, property))
			.collect::<Result<Vec<_>>>()?;

		Ok(Column {
			id,
			name: column_to_create.column,
			constraint: column_to_create.constraint,
			index: column_to_create.index,
			properties,
			auto_increment: column_to_create.auto_increment,
			dictionary_id: column_to_create.dictionary_id,
		})
	}
}

#[cfg(test)]
pub mod test {
	use reifydb_codec::row::catalog::EncodedCatalogRow;
	use reifydb_core::{
		interface::catalog::{
			column::ColumnIndex,
			id::{ColumnId, TableId},
			property::{ColumnPropertyKind, ColumnSaturationStrategy},
		},
		key::column::ColumnsKey,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use crate::{
		CatalogStore,
		store::column::{create::ColumnToCreate, shape::column},
		test_utils::ensure_test_table,
	};

	#[test]
	fn test_create_column() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "col_1".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Boolean),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "col_2".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Int2),
				properties: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let column_1 = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(16385)).unwrap();

		assert_eq!(column_1.id, 16385);
		assert_eq!(column_1.name, "col_1");
		assert_eq!(column_1.constraint.get_type(), ValueType::Boolean);
		assert!(!column_1.auto_increment);

		let column_2 = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(16386)).unwrap();

		assert_eq!(column_2.id, 16386);
		assert_eq!(column_2.name, "col_2");
		assert_eq!(column_2.constraint.get_type(), ValueType::Int2);
		assert!(!column_2.auto_increment);
	}

	#[test]
	fn test_create_column_with_auto_increment() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Uint8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.unwrap();

		let column = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(16385)).unwrap();

		assert_eq!(column.id, ColumnId(16385));
		assert_eq!(column.name, "id");
		assert_eq!(column.constraint.get_type(), ValueType::Uint8);
		assert!(column.auto_increment);
	}

	#[test]
	fn test_auto_increment_invalid_type() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "name".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Utf8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.unwrap_err();

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_109");
		assert!(diagnostic.message.contains("auto increment is not supported for type"));

		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "is_active".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Boolean),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_109");

		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "price".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Float8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_109");
	}

	#[test]
	fn test_column_already_exists() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "col_1".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Boolean),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "col_1".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Boolean),
				properties: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap_err();

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_107");
	}

	fn stored_type_and_constraint(txn: &mut AdminTransaction, id: ColumnId) -> (Vec<u8>, Vec<u8>) {
		let multi = Transaction::Admin(&mut *txn)
			.get(&[ColumnsKey::new(id).into()])
			.unwrap()
			.pop()
			.flatten()
			.unwrap();
		let row = EncodedCatalogRow::try_from(multi.bytes).unwrap();
		(column::get_value_type(&row).as_bytes().to_vec(), column::get_constraint(&row).as_bytes().to_vec())
	}

	#[test]
	fn test_digest_column_constraint_bytes_are_pinned() {
		// The full type is on disk as typeinfo bytes; any change strands stored digest columns.
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		let mut create = |name: &str, index: u8, ty: ValueType| {
			CatalogStore::create_column(
				&mut txn,
				TableId(1),
				ColumnToCreate {
					fragment: None,
					namespace_name: "test_namespace".to_string(),
					object_name: "test_table".to_string(),
					column: name.to_string(),
					constraint: TypeConstraint::unconstrained(ty),
					properties: vec![],
					index: ColumnIndex(index),
					auto_increment: false,
					dictionary_id: None,
				},
			)
			.unwrap()
			.id
		};
		let plain = create(
			"plain",
			0,
			ValueType::Digest {
				inner: Box::new(ValueType::Duration),
				accuracy: 10_000,
			},
		);
		let optional = create(
			"optional",
			1,
			ValueType::Option(Box::new(ValueType::Digest {
				inner: Box::new(ValueType::Uint16),
				accuracy: 100_000,
			})),
		);

		assert_eq!(stored_type_and_constraint(&mut txn, plain), (vec![30, 18, 0x10, 0x27, 0, 0], vec![0]));
		assert_eq!(
			stored_type_and_constraint(&mut txn, optional),
			(vec![0x40 | 30, 14, 0xa0, 0x86, 0x01, 0x00], vec![0])
		);
	}

	#[test]
	fn a_created_column_reports_exactly_the_properties_stored_for_it() {
		// The returned column must match storage, otherwise callers act on properties a reload would not see.
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		let created = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "saturating".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Int2),
				properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::Error)],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let stored =
			CatalogStore::list_column_properties(&mut Transaction::Admin(&mut txn), created.id).unwrap();

		assert_eq!(stored.len(), 1, "exactly one property must be stored for the column");
		assert_eq!(created.properties.len(), stored.len());
		assert_eq!(created.properties[0].id, stored[0].id);
		assert_eq!(created.properties[0].column, stored[0].column);
		assert_eq!(created.properties[0].property.to_u8(), stored[0].property.to_u8());
	}
}
