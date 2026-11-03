// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::utf8;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, broadcast::row_of, context::FunctionContext,
	error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::durations,
	duration::Duration,
	value_type::ValueType,
};

const NANOS_PER_DAY: u128 = 86_400_000_000_000;

const UNITS: [(u128, &str); 6] = [
	(1_000, "us"),
	(1_000_000, "ms"),
	(1_000_000_000, "s"),
	(60_000_000_000, "m"),
	(3_600_000_000_000, "h"),
	(NANOS_PER_DAY, "d"),
];

pub struct DurationHumanize {
	info: RoutineInfo,
}

impl Default for DurationHumanize {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationHumanize {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::humanize"),
		}
	}
}

fn hundredths(nanos: u128, per_unit: u128) -> u128 {
	(nanos * 100 + per_unit / 2) / per_unit
}

fn humanize(duration: &Duration) -> String {
	if duration.get_months() != 0 {
		return duration.to_string();
	}
	let total = duration.get_days() as i128 * NANOS_PER_DAY as i128 + duration.get_nanos() as i128;
	if total == 0 {
		return "0s".to_string();
	}
	let sign = if total < 0 {
		"-"
	} else {
		""
	};
	let nanos = total.unsigned_abs();
	let Some(mut unit) = UNITS.iter().rposition(|(per_unit, _)| nanos >= *per_unit) else {
		return format!("{sign}{nanos}ns");
	};
	if let Some((next, _)) = UNITS.get(unit + 1)
		&& hundredths(nanos, UNITS[unit].0) >= next * 100 / UNITS[unit].0
	{
		unit += 1;
	}
	let (per_unit, name) = UNITS[unit];
	let rounded = hundredths(nanos, per_unit);
	format!("{sign}{}.{:02}{name}", rounded / 100, rounded % 100)
}

impl<'a> Routine<FunctionContext<'a>> for DurationHumanize {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		match &data.data {
			ViewData::Duration(container) => {
				let values = durations(container);
				let rendered: Vec<String> = (0..ctx.row_count)
					.map(|row| humanize(&values[row_of(values.len(), row)]))
					.collect();
				Ok(utf8(ctx.fragment.text(), rendered))
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Duration],
				actual: data.get_type(),
			}),
		}
	}
}

impl Function for DurationHumanize {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
