// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::once;

use arrow_array::RecordBatch;
use reifydb_catalog::{
	catalog::series::SeriesToCreate,
	store::{row_settings::create::create_row_settings, series::shape::deserialize_encoding_config},
};
use reifydb_core::{interface::catalog::storage::StorageId, row::RowSettings, value::batch::single_row};
use reifydb_extension::block_encoding::error::BlockEncodingError;
use reifydb_rql::nodes::CreateSeriesNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_series(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSeriesNode,
) -> Result<RecordBatch> {
	let result = services.catalog.create_series(
		txn,
		SeriesToCreate {
			name: plan.series.clone(),
			namespace: plan.namespace.def().id(),
			columns: plan.columns,
			tag: plan.tag,
			key: plan.key,
			partition_by: plan.partition_by.clone(),
			time: plan.time.clone(),
			encoding: plan.encoding.as_ref().map(|(_, encoding)| encoding.clone()),
		},
	)?;

	if let Some((fragment, encoding)) = &plan.encoding {
		let Some(block_encoding) = services.block_encodings.get(&encoding.name) else {
			return Err(BlockEncodingError::UnknownName {
				name: encoding.name.clone(),
				fragment: fragment.clone(),
			}
			.into());
		};
		let config = deserialize_encoding_config(&encoding.name, &encoding.config)?;
		let user_fields = result.encoding_user_fields();
		let bound = block_encoding.bind(&user_fields, &config)?;
		let stored_fields = bound.stored_fields();
		for column in once(result.key.column()).chain(result.tag.is_some().then_some("tag")) {
			let changed = || BlockEncodingError::PassThroughFieldChanged {
				name: encoding.name.clone(),
				column: column.to_string(),
			};
			let user_field = user_fields.iter().find(|field| field.name() == column).ok_or_else(changed)?;
			if !stored_fields.iter().any(|field| field == user_field) {
				return Err(changed().into());
			}
		}
	}

	if let Some(ttl) = plan.ttl {
		create_row_settings(
			txn,
			StorageId::Series(result.id),
			&RowSettings {
				ttl: Some(ttl),
				persistent: plan.persistent,
			},
		)?;
	}

	single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("series", Value::Utf8(plan.series.text().to_string())),
		("created", Value::Boolean(true)),
	])
}
