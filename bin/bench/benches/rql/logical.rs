//! # RQL Parsing Benchmarks
//!
//! Measures the performance of RQL parsing (tokens → AST).
//!
//! Run with: `cargo bench --bench rql-parse`

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use reifydb_bench::queries;
use reifydb_rql::{ast::parse_str, plan::logical_all};

fn bench_parsing(c: &mut Criterion) {
	let mut group = c.benchmark_group("rql_logical");
	group.sample_size(1000);
	group.measurement_time(Duration::from_secs(10));
	group.warm_up_time(Duration::from_secs(3));
	group.throughput(Throughput::Elements(1));

	group.bench_function("MAP_ONE", |b| {
		b.iter_with_setup(
			|| parse_str(queries::MAP_ONE).unwrap(),
			|statements| logical_all(statements),
		)
	});

	group.bench_function("INLINE_DATA", |b| {
		b.iter_with_setup(
			|| parse_str(queries::INLINE_DATA).unwrap(),
			|statements| logical_all(statements),
		)
	});

	group.bench_function("SIMPLE_FILTER", |b| {
		b.iter_with_setup(
			|| parse_str(queries::SIMPLE_FILTER).unwrap(),
			|statements| logical_all(statements),
		)
	});

	group.bench_function("COMPLEX_FILTER", |b| {
		b.iter_with_setup(
			|| parse_str(queries::COMPLEX_FILTER).unwrap(),
			|statements| logical_all(statements),
		)
	});

	group.finish();
}

criterion_group!(benches, bench_parsing);
criterion_main!(benches);
