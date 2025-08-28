//! # RQL Parsing Benchmarks
//!
//! Measures the performance of RQL parsing (tokens → AST).
//!
//! Run with: `cargo bench --bench rql-parse`

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use reifydb_bench::queries;
use reifydb_rql::ast::{parse, tokenize};

fn bench_parsing(c: &mut Criterion) {
	let mut group = c.benchmark_group("rql_parsing");
	group.sample_size(1000);
	group.measurement_time(Duration::from_secs(10));
	group.warm_up_time(Duration::from_secs(3));
	group.throughput(Throughput::Elements(1));

	group.bench_function("MAP_ONE", |b| {
		b.iter_with_setup(
			|| tokenize(queries::MAP_ONE).unwrap(),
			|tokens| parse(tokens),
		)
	});

	group.bench_function("INLINE_DATA", |b| {
		b.iter_with_setup(
			|| tokenize(queries::INLINE_DATA).unwrap(),
			|tokens| parse(tokens),
		)
	});

	group.bench_function("SIMPLE_FILTER", |b| {
		b.iter_with_setup(
			|| tokenize(queries::SIMPLE_FILTER).unwrap(),
			|tokens| parse(tokens),
		)
	});

	group.bench_function("COMPLEX_FILTER", |b| {
		b.iter_with_setup(
			|| tokenize(queries::COMPLEX_FILTER).unwrap(),
			|tokens| parse(tokens),
		)
	});

	group.finish();
}

criterion_group!(benches, bench_parsing);
criterion_main!(benches);
