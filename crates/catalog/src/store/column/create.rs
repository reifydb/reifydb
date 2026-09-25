// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{constraint::encode_type_constraint, tag::type_tag_byte};
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

fn encode_constraint(type_constraint: &TypeConstraint) -> Vec<u8> {
	let encoded = encode_type_constraint(type_constraint).unwrap_or_else(|error| {
		panic!("column type {} cannot be encoded: {error}", type_constraint.get_type())
	});
	match type_constraint.constraint() {
		None if matches!(encoded.constraint_type, 2 | 5) => {
			let mut bytes = vec![encoded.constraint_type];
			bytes.extend_from_slice(&encoded.constraint_param1.to_le_bytes());
			bytes.extend_from_slice(&encoded.constraint_param2.to_le_bytes());
			bytes
		}
		None => vec![0],
		Some(Constraint::MaxBytes(max_bytes)) => {
			let mut bytes = vec![1];
			let max_value: u32 = (*max_bytes).into();
			bytes.extend_from_slice(&max_value.to_le_bytes());
			bytes
		}
		Some(Constraint::Dictionary(dict_id, id_type)) => {
			let mut bytes = vec![3];
			bytes.extend_from_slice(&dict_id.to_u64().to_le_bytes());
			bytes.push(type_tag_byte(id_type));
			bytes
		}
		Some(Constraint::SumType(id)) => {
			let mut bytes = vec![4];
			bytes.extend_from_slice(&id.to_u64().to_le_bytes());
			bytes
		}
	}
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
		column_to_create: ColumnToCreate,
	) -> Result<Column> {
		let object = object.into();

		Self::reject_existing_column(txn, object, &column_to_create)?;
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
		column_to_create: ColumnToCreate,
	) -> Result<Column> {
		let object = object.into();

		Self::store_column_row(txn, id, object, &column_to_create)?;
		Self::store_object_column_row(txn, id, object, &column_to_create)?;

		Self::create_properties_and_build(txn, id, column_to_create)
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
				| ValueType::Int2
				| ValueType::Int4
				| ValueType::Int8
				| ValueType::Int16
				| ValueType::Uint1
				| ValueType::Uint2
				| ValueType::Uint4
				| ValueType::Uint8
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
		column::set_value(&mut row, type_tag_byte(&column_to_create.constraint.get_type()));
		column::set_index(&mut row, u8::from(column_to_create.index));
		column::set_auto_increment(&mut row, column_to_create.auto_increment);

		let constraint_bytes = encode_constraint(&column_to_create.constraint);
		let blob = Blob::from(constraint_bytes);
		column::set_constraint(&mut row, &blob);

		let dict_id_value = column_to_create.dictionary_id.map(u64::from).unwrap_or(0);
		column::set_dictionary_id(&mut row, dict_id_value);

		txn.set(&ColumnsKey::new(id), row.freeze())
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
		txn.set(&ColumnKey::new(object, id), row.freeze())
	}

	fn create_properties_and_build(
		txn: &mut AdminTransaction,
		id: ColumnId,
		column_to_create: ColumnToCreate,
	) -> Result<Column> {
		for policy in column_to_create.properties {
			Self::create_column_property(txn, id, policy)?;
		}

		Ok(Column {
			id,
			name: column_to_create.column,
			constraint: column_to_create.constraint,
			index: column_to_create.index,
			properties: Self::list_column_properties(&mut Transaction::Admin(&mut *txn), id)?,
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
		assert_eq!(column_1.auto_increment, false);

		let column_2 = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(16386)).unwrap();

		assert_eq!(column_2.id, 16386);
		assert_eq!(column_2.name, "col_2");
		assert_eq!(column_2.constraint.get_type(), ValueType::Int2);
		assert_eq!(column_2.auto_increment, false);
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
		assert_eq!(column.auto_increment, true);
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
		assert_eq!(diagnostic.code, "CA_006");
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

		assert_eq!(err.diagnostic().code, "CA_006");

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

		assert_eq!(err.diagnostic().code, "CA_006");
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
		assert_eq!(diagnostic.code, "CA_005");
	}

	fn stored_type_and_constraint(txn: &mut AdminTransaction, id: ColumnId) -> (u8, Vec<u8>) {
		let multi = Transaction::Admin(&mut *txn).get(&ColumnsKey::new(id)).unwrap().unwrap();
		let row = EncodedCatalogRow::try_from(multi.bytes).unwrap();
		(column::get_value(&row), column::get_constraint(&row).as_bytes().to_vec())
	}

	#[test]
	fn test_digest_column_constraint_bytes_are_pinned() {
		// Tag 5, ppm then inner type tag as u32 LE is on disk; any change strands stored digest columns.
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

		assert_eq!(stored_type_and_constraint(&mut txn, plain), (32, vec![5, 0x10, 0x27, 0, 0, 18, 0, 0, 0]));
		assert_eq!(
			stored_type_and_constraint(&mut txn, optional),
			(0x40 | 32, vec![5, 0xa0, 0x86, 0x01, 0x00, 14, 0, 0, 0])
		);
	}
}
