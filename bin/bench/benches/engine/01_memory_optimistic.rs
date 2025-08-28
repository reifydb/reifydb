//! # Memory Optimistic Transaction Benchmarks
//!
//! Run with: `cargo bench --bench engine-memory-optimistic`

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use reifydb::{sync, MemoryDatabaseOptimistic, Params, SessionSync};
use std::time::Duration;

fn bench_simple_queries(c: &mut Criterion) {
	let db = create_benchmark_db_with_data();
	let mut group = c.benchmark_group("query");

	group.sample_size(1000);
	group.measurement_time(Duration::from_secs(10));
	group.warm_up_time(Duration::from_secs(3));
	group.throughput(Throughput::Elements(1));

	group.bench_function("MAP", |b| {
		b.iter(|| db.query_as_root("MAP { 1 }", Params::None).unwrap())
	});

	group.bench_function("inline_data", |b| {
		b.iter(|| {
			db.query_as_root(
				"from [{ id: 1, name: \"test\" }]",
				Params::None,
			)
			.unwrap()
		})
	});

	group.bench_function("filter", |b| {
		b.iter(|| {
			db.query_as_root("from [{ id: 1, active: true }, { id: 2, active: false }] filter active = true", Params::None)
				.unwrap()
		})
	});

	group.bench_function("filter_large", |b| {
		let large_query = generate_large_filter_query();
		b.iter(|| {
			db.query_as_root(&large_query, Params::None)
				.unwrap()
		})
	});

	group.finish();
}

fn generate_large_filter_query() -> String {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};
	
	let mut items = Vec::new();
	
	for i in 1..=2049 {
		let mut hasher = DefaultHasher::new();
		i.hash(&mut hasher);
		let hash = hasher.finish();
		
		let active = if i <= 1025 {
			(hash % 2) == 0
		} else {
			(hash % 2) == 1
		};
		
		items.push(format!("{{ id: {}, active: {} }}", i, active));
	}
	
	format!("from [{}] filter active = true", items.join(", "))
}

criterion_group!(benches, bench_simple_queries);
criterion_main!(benches);

fn create_benchmark_db() -> MemoryDatabaseOptimistic {
	let mut db = sync::memory_optimistic().build().unwrap();
	db.start().unwrap();
	db
}

fn create_benchmark_db_with_data() -> MemoryDatabaseOptimistic {
	let db = create_benchmark_db();

	// Create schema and table
	// db.command_as_root(r#"create schema demo"#, Params::None).unwrap();
	//
	// db.command_as_root(
	// 	r#"
	// 	create table demo.users {
	// 		id: int4,
	// 		username: utf8,
	// 		email: utf8,
	// 		age: int4,
	// 		is_active: bool
	// 	}
	// 	"#,
	// 	Params::None,
	// ).unwrap();
	//
	// // Insert sample data - enough for meaningful benchmarks
	// db.command_as_root(
	// 	r#"
	// 	from [
	// 		{ id: 1, username: "alice", email: "alice@example.com", age: 30, is_active: true },
	// 		{ id: 2, username: "bob", email: "bob@example.com", age: 25, is_active: true },
	// 		{ id: 3, username: "charlie", email: "charlie@example.com", age: 35, is_active: false },
	// 		{ id: 4, username: "diana", email: "diana@example.com", age: 28, is_active: true },
	// 		{ id: 5, username: "eve", email: "eve@example.com", age: 32, is_active: true },
	// 		{ id: 6, username: "frank", email: "frank@example.com", age: 29, is_active: false },
	// 		{ id: 7, username: "grace", email: "grace@example.com", age: 27, is_active: true },
	// 		{ id: 8, username: "henry", email: "henry@example.com", age: 33, is_active: true },
	// 		{ id: 9, username: "iris", email: "iris@example.com", age: 26, is_active: false },
	// 		{ id: 10, username: "jack", email: "jack@example.com", age: 31, is_active: true }
	// 	]
	// 	insert demo.users
	// 	"#,
	// 	Params::None,
	// ).unwrap();

	db
}
