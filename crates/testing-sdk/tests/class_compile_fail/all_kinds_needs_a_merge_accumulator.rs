use std::collections::BTreeMap;

use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::ApplyWith,
};
use reifydb_flow::window::{accumulator::invertible::retained_map::RetainedAccumulator, span::WindowSpan};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		view::RowView,
		windowed::operator::{AllKinds, Emit, WindowSettings, WindowedOperator},
	},
	row,
};
use reifydb_value::{config::ExtensionParams, value::datetime::DateTime};

#[derive(Clone, Debug, PartialEq)]
struct Out {
	group: String,
}

row!(Out { group: String });

struct Unmergeable;

impl OperatorMetadata for Unmergeable {
	const NAME: &'static str = "unmergeable";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "rolling over an accumulator that cannot merge panes";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for Unmergeable {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = Out;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, _row: &impl RowView) -> Option<(String, (u64, f64))> {
		None
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl Emit for Unmergeable {
	type Kinds = AllKinds;

	fn build_output(&self, group: &String, _span: WindowSpan<DateTime>, _value: &BTreeMap<u64, f64>) -> Option<Out> {
		Some(Out {
			group: group.clone(),
		})
	}
}

fn main() {}
