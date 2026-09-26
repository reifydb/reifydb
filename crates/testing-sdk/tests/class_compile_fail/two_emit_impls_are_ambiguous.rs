// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::ApplyWith,
};
use reifydb_flow_async::window::{
	accumulator::invertible::retained_map::RetainedAccumulator, settings::WindowSettings, span::WindowSpan,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		view::RowView,
		windowed::operator::{CarryEmit, Emit, NoRolling, WindowDriver, WindowedOperator},
	},
	row,
};
use reifydb_value::{config::ExtensionParams, value::datetime::DateTime};

#[derive(Clone, Debug, PartialEq)]
struct Out {
	group: String,
}

row!(Out { group: String });

struct Both;

impl OperatorMetadata for Both {
	const NAME: &'static str = "both";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "implements both Emit and CarryEmit";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for Both {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = Out;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Result<Option<DateTime>> {
		Ok(row.row_time())
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, _row: &impl RowView) -> Result<Option<(String, (u64, f64))>> {
		Ok(None)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl Emit for Both {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, _span: WindowSpan<DateTime>, _value: &BTreeMap<u64, f64>) -> Option<Out> {
		Some(Out {
			group: group.clone(),
		})
	}
}

impl CarryEmit for Both {
	type Carry = f64;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<DateTime>,
		_value: &BTreeMap<u64, f64>,
		_prev: Option<&f64>,
	) -> Option<Out> {
		Some(Out {
			group: group.clone(),
		})
	}

	fn carry_forward(&self, _value: &BTreeMap<u64, f64>, _prev: Option<&f64>) -> Option<f64> {
		None
	}
}

fn main() {
	let _driver: Option<<Both as WindowDriver<_>>::Driver> = None;
}
