use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::ApplyWith,
};
use reifydb_flow::window::{accumulator::WindowAccumulator, span::WindowSpan};
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

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default)]
struct NoMerge {
	total: f64,
}

impl HeapSize for NoMerge {
	fn heap_size(&self) -> usize {
		0
	}
}

impl WindowAccumulator for NoMerge {
	type Contribution = f64;
	type Output = f64;

	fn add(&mut self, contribution: &f64) {
		self.total += contribution;
	}

	fn remove(&mut self, contribution: &f64) {
		self.total -= contribution;
	}

	fn finalize(&self) -> Option<f64> {
		Some(self.total)
	}

	fn is_empty(&self) -> bool {
		self.total == 0.0
	}
}

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
	type Accumulator = NoMerge;
	type Output = Out;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, _row: &impl RowView) -> Option<(String, f64)> {
		None
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> NoMerge {
		NoMerge::default()
	}
}

impl Emit for Unmergeable {
	type Kinds = AllKinds;

	fn build_output(&self, group: &String, _span: WindowSpan<DateTime>, _value: &f64) -> Option<Out> {
		Some(Out {
			group: group.clone(),
		})
	}
}

fn main() {}
