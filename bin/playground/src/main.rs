// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{
	MemoryDatabaseOptimistic, SessionSync, core::interface::Params, sync,
};

pub type DB = MemoryDatabaseOptimistic;
// pub type DB = SqliteDatabaseOptimistic;

fn main() {
	let mut db: DB = sync::memory_optimistic();
	// let mut db: DB =
	// sync::sqlite_optimistic(SqliteConfig::new("/tmp/reifydb"));

	db.start().unwrap();

	db.command_as_root(
		r#"
	    create schema test;
	    create table test.users { name: utf8, age: int1};
	"#,
		Params::None,
	)
	.unwrap();

	// Skip computed view for now since flow subsystem has unimplemented
	db.command_as_root(
		r#"
create computed view test.basic { name: utf8, age: int1 } with {
    from test.users
}
	"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
    from [
        { name: "bob", age: 17 },
        { name: "lucy", age: 20 },
        { name: "juciy", age: 19 },
    ]
    insert test.users;

    "#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
	from [
	    { name: "dim", age: 40 },
	]
	insert test.users;

	"#,
		Params::None,
	)
	.unwrap();

	// for frame in
	// 	db.query_as_root(r#"FROM test.users"#, Params::None).unwrap()
	// {
	// 	println!("{}", frame);
	// }

	// db.command_as_root(
	// 	r#"
	// from test.users
	// filter { name = "bob" }
	// map { name: "bob", age: 21}
	// update test.users;
	//
	// "#,
	// 	Params::None,
	// )
	// .unwrap();

	// for frame in
	// 	db.query_as_root(r#"FROM test.users"#, Params::None).unwrap()
	// {
	// 	println!("{}", frame);
	// }

	// loop {}
	// thread::sleep(Duration::from_millis(10));

	// println!("Basic database operations completed successfully!");
	// rql_to_flow_example(&mut db);

	for frame in
		db.query_as_root(r#"FROM test.basic"#, Params::None).unwrap()
	{
		println!("{}", frame);
	}
}
