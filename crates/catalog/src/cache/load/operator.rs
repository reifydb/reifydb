// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::flow::{FlowId, Operator, OperatorId},
		store::MultiVersionRow,
	},
	key::operator::OperatorKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{Result, store::operator::shape::operator};

pub(crate) fn load_operators(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = OperatorKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let operator = convert_operator(multi)?;
		catalog.set_operator(operator.id, version, Some(operator));
	}

	Ok(())
}

fn convert_operator(multi: MultiVersionRow) -> Result<Operator> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = OperatorId(operator::get_id(&bytes));
	let flow = FlowId(operator::get_flow(&bytes));
	let node_type = operator::get_type(&bytes);
	let data = operator::get_data(&bytes).clone();

	Ok(Operator {
		id,
		flow,
		node_type,
		data,
	})
}
