// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[allow(clippy::disallowed_types)]
use std::time::Duration;
use std::{hint::black_box, time::Instant};

use reifydb_benches::{BenchReport, env_usize};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_evaluate::expression::compare::{CompareOp, Equal, LessThan, compare_columns};
use reifydb_value::{error::Diagnostic, fragment::Fragment, value::value_type::ValueType};

#[allow(clippy::disallowed_types)]
fn measure<Op: CompareOp>(repeats: usize, left: &ColumnWithName, right: &ColumnWithName) -> Duration {
	let mut samples: Vec<Duration> = (0..repeats)
		.map(|_| {
			let started = Instant::now();
			let output = compare_columns::<Op>(
				left,
				right,
				Fragment::internal("bench"),
				|_, l: ValueType, r: ValueType| -> Diagnostic {
					panic!("no comparison between {l:?} and {r:?}")
				},
			)
			.expect("bench comparison succeeds");
			let elapsed = started.elapsed();
			drop(black_box(output));
			elapsed
		})
		.collect();
	samples.sort();
	samples[samples.len() / 2]
}

fn bench_case(
	report: &mut BenchReport,
	label: &str,
	rows: usize,
	repeats: usize,
	left: ColumnBuffer,
	right: ColumnBuffer,
) {
	let left = ColumnWithName::new("left", left);
	let right = ColumnWithName::new("right", right);
	report.record_throughput(&format!("{label}/eq"), rows as u64, measure::<Equal>(repeats, &left, &right));
	report.record_throughput(&format!("{label}/lt"), rows as u64, measure::<LessThan>(repeats, &left, &right));
}

fn main() {
	let rows = env_usize("ROWS", 1_000_000);
	let repeats = env_usize("REPEATS", 20);
	println!("rows={rows} repeats={repeats}");

	let ints = || ColumnBuffer::int4((0..rows).map(|i| i as i32));
	let floats = || ColumnBuffer::float8((0..rows).map(|i| i as f64 * 0.5));
	let strings = || ColumnBuffer::utf8((0..rows).map(|i| format!("k{i:08}")));

	let mut report = BenchReport::new("compare");
	bench_case(&mut report, "int4-int4", rows, repeats, ints(), ints());
	bench_case(&mut report, "int4-int8", rows, repeats, ints(), ColumnBuffer::int8((0..rows).map(|i| i as i64)));
	bench_case(&mut report, "int4-literal", rows, repeats, ints(), ColumnBuffer::int4(vec![500]));
	bench_case(&mut report, "float8-float8", rows, repeats, floats(), floats());
	bench_case(&mut report, "utf8-utf8", rows, repeats, strings(), strings());
	report.save();
}
