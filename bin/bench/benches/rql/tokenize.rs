//! # RQL Tokenization Benchmarks
//!
//! Measures the performance of RQL lexical analysis (string → tokens).
//!
//! Run with: `cargo bench --bench rql-tokenize`

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use reifydb_bench::queries;
use reifydb_rql::ast::tokenize;

fn bench_tokenization(c: &mut Criterion) {
	let mut group = c.benchmark_group("rql_tokenization");
	group.sample_size(1000);
	group.measurement_time(Duration::from_secs(10));
	group.warm_up_time(Duration::from_secs(3));
	group.throughput(Throughput::Elements(1));

	group.bench_function("MAP_ONE", |b| {
		b.iter(|| tokenize(queries::MAP_ONE).unwrap().len())
	});

	group.bench_function("INLINE_DATA", |b| {
		b.iter(|| tokenize(queries::INLINE_DATA).unwrap().len())
	});

	group.bench_function("SIMPLE_FILTER", |b| {
		b.iter(|| tokenize(queries::SIMPLE_FILTER).unwrap().len())
	});

	group.bench_function("COMPLEX_FILTER", |b| {
		b.iter(|| tokenize(queries::COMPLEX_FILTER).unwrap().len())
	});

	group.finish();
}

criterion_group!(benches, bench_tokenization);
criterion_main!(benches);
