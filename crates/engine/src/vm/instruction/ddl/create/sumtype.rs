// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::catalog::{
	ringbuffer::RingBufferColumnToCreate, sumtype::SumTypeToCreate, table::TableColumnToCreate,
};
use reifydb_core::{
	interface::catalog::sumtype::{Field, SumTypeKind, Variant},
	value::batch::single_row,
};
use reifydb_rql::nodes::CreateSumTypeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		constraint::{Constraint, TypeConstraint},
		value_type::ValueType,
	},
};

use crate::{Result, vm::services::Services};

pub(super) trait SumTypeExpandable: Sized {
	fn name(&self) -> &Fragment;
	fn fragment(&self) -> &Fragment;
	fn constraint(&self) -> &TypeConstraint;
	fn expanded(name: String, fragment: Fragment, constraint: TypeConstraint) -> Self;
}

impl SumTypeExpandable for TableColumnToCreate {
	fn name(&self) -> &Fragment {
		&self.name
	}

	fn fragment(&self) -> &Fragment {
		&self.fragment
	}

	fn constraint(&self) -> &TypeConstraint {
		&self.constraint
	}

	fn expanded(name: String, fragment: Fragment, constraint: TypeConstraint) -> Self {
		Self {
			name: Fragment::internal(name),
			fragment,
			constraint,
			properties: vec![],
			auto_increment: false,
			dictionary_id: None,
		}
	}
}

impl SumTypeExpandable for RingBufferColumnToCreate {
	fn name(&self) -> &Fragment {
		&self.name
	}

	fn fragment(&self) -> &Fragment {
		&self.fragment
	}

	fn constraint(&self) -> &TypeConstraint {
		&self.constraint
	}

	fn expanded(name: String, fragment: Fragment, constraint: TypeConstraint) -> Self {
		Self {
			name: Fragment::internal(name),
			fragment,
			constraint,
			properties: vec![],
			auto_increment: false,
			dictionary_id: None,
		}
	}
}

pub(super) fn expand_sumtype_columns<C: SumTypeExpandable>(
	services: &Services,
	txn: &mut AdminTransaction,
	columns: Vec<C>,
) -> Result<Vec<C>> {
	let mut expanded = Vec::with_capacity(columns.len());

	for col in columns {
		match col.constraint().constraint() {
			Some(Constraint::SumType(id)) => {
				let def = services.catalog.get_sumtype(&mut Transaction::Admin(&mut *txn), *id)?;
				let col_name = col.name().text();

				expanded.push(C::expanded(
					format!("{col_name}_tag"),
					col.fragment().clone(),
					TypeConstraint::with_constraint(
						col.constraint().get_type(),
						Constraint::SumType(*id),
					),
				));

				for variant in &def.variants {
					for field in &variant.fields {
						let field_type = match field.field_type.get_type() {
							optional @ ValueType::Option(_) => optional,
							other => ValueType::Option(Box::new(other)),
						};
						expanded.push(C::expanded(
							format!("{col_name}_{}_{}", variant.name, field.name),
							col.fragment().clone(),
							TypeConstraint::unconstrained(field_type),
						));
					}
				}
			}
			_ => {
				expanded.push(col);
			}
		}
	}

	Ok(expanded)
}

pub(crate) fn create_sumtype(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSumTypeNode,
) -> Result<RecordBatch> {
	if let Some(existing) = services.catalog.find_sumtype_by_name(
		&mut Transaction::Admin(&mut *txn),
		plan.namespace.id(),
		plan.name.text(),
	)? && plan.if_not_exists
	{
		return single_row([
			("id", Value::Uint8(existing.id.0)),
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("enum", Value::Utf8(plan.name.text().to_string())),
			("created", Value::Boolean(false)),
		]);
	}

	let mut variants = Vec::with_capacity(plan.variants.len());
	for (tag, variant) in plan.variants.iter().enumerate() {
		let mut fields = Vec::with_capacity(variant.columns.len());
		for col in &variant.columns {
			fields.push(Field {
				name: col.name.to_lowercase(),
				field_type: col.column_type.clone(),
			});
		}
		variants.push(Variant {
			tag: tag as u8,
			name: variant.name.to_lowercase(),
			fields,
		});
	}

	let result = services.catalog.create_sumtype(
		txn,
		SumTypeToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			variants,
			kind: SumTypeKind::Enum,
		},
	)?;

	single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("enum", Value::Utf8(plan.name.text().to_string())),
		("created", Value::Boolean(true)),
	])
}
