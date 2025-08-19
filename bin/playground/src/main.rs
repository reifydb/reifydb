// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{rc::Rc, thread, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, SessionSync,
	core::{
		interceptor::{
			Interceptors, PreCommitContext, PreCommitInterceptor,
			RegisterInterceptor, TablePostInsertContext,
			TablePostInsertInterceptor, TablePreInsertContext,
			TablePreInsertInterceptor, post_commit,
			table_post_insert, table_pre_insert,
		},
		interface::{Params, Transaction},
	},
	sync,
};

pub type DB = MemoryDatabaseOptimistic;

// Example: Multi-trait interceptor that implements multiple interceptor
// interfaces
#[derive(Clone)]
struct AuditInterceptor {
	name: String,
}

impl AuditInterceptor {
	fn new(name: impl Into<String>) -> Self {
		Self {
			name: name.into(),
		}
	}
}

// Implement multiple interceptor traits
impl<T: Transaction> TablePreInsertInterceptor<T> for AuditInterceptor {
	fn intercept(
		&self,
		_ctx: &mut TablePreInsertContext<T>,
	) -> reifydb::core::Result<()> {
		println!(
			"[{}] Audit: Pre-insert interceptor called",
			self.name
		);
		Ok(())
	}
}

impl<T: Transaction> TablePostInsertInterceptor<T> for AuditInterceptor {
	fn intercept(
		&self,
		_ctx: &mut TablePostInsertContext<T>,
	) -> reifydb::core::Result<()> {
		println!(
			"[{}] Audit: Post-insert interceptor called",
			self.name
		);
		Ok(())
	}
}

impl<T: Transaction> PreCommitInterceptor<T> for AuditInterceptor {
	fn intercept(
		&self,
		_ctx: &mut PreCommitContext<T>,
	) -> reifydb::core::Result<()> {
		println!(
			"[{}] Audit: Pre-commit interceptor called",
			self.name
		);
		Ok(())
	}
}

// Implement RegisterInterceptor to handle multi-trait registration
impl<T: Transaction> RegisterInterceptor<T> for AuditInterceptor {
	fn register(self: Rc<Self>, interceptors: &mut Interceptors<T>) {
		interceptors.table_pre_insert.add(self.clone());
		interceptors.table_post_insert.add(self.clone());
		interceptors.pre_commit.add(self);
	}
}
// pub type DB = SqliteDatabaseOptimistic;

fn main() {
	// Example: Using the new unified interceptor API
	let mut db: DB = sync::memory_optimistic()
		.intercept(table_pre_insert(|_ctx| {
			println!("Table pre insert interceptor called!");

			Ok(())
		}))
		.intercept(table_post_insert(|_ctx| {
			println!("Table post insert interceptor called!");
			Ok(())
		}))
		.intercept(post_commit(|ctx| {
			println!(
				"Post-commit interceptor called with version: {:?}",
				ctx.version
			);
			Ok(())
		}))
		// Example: Using a multi-trait interceptor
		.intercept(AuditInterceptor::new("MyAudit"))
		.build()
		.unwrap();
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

	db.command_as_root(
		r#"
create deferred view test.basic { name: utf8, age: int1 } with {
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
	thread::sleep(Duration::from_millis(10));

	// println!("Basic database operations completed successfully!");
	// rql_to_flow_example(&mut db);

	for frame in
		db.query_as_root(r#"FROM test.basic"#, Params::None).unwrap()
	{
		println!("{}", frame);
	}
}
