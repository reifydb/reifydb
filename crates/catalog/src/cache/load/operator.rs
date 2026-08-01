// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::{FlowId, Operator, OperatorId},
		store::MultiVersionRow,
	},
	key::operator::OperatorKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	Result,
	store::operator::shape::operator::{self, DATA, FLOW, ID, TYPE},
};

pub(crate) fn load_operators(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = OperatorKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let operator = convert_operator(multi);
		catalog.set_operator(operator.id, version, Some(operator));
	}

	Ok(())
}

fn convert_operator(multi: MultiVersionRow) -> Operator {
	let row = multi.row;
	let id = OperatorId(operator::SHAPE.get::<u64>(&row, ID));
	let flow = FlowId(operator::SHAPE.get::<u64>(&row, FLOW));
	let node_type = operator::SHAPE.get::<u8>(&row, TYPE);
	let data = operator::SHAPE.get_blob(&row, DATA).clone();

	Operator {
		id,
		flow,
		node_type,
		data,
	}
}
