//! # Basic Tables Example
//!
//! Demonstrates table operations in ReifyDB:
//! - Creating schemas and tables
//! - Working with different data types
//! - Insert, update, and delete operations
//! - Querying with filters
//!
//! Run with: `make basic-tables` or `cargo run --bin basic-tables`

use reifydb::core::interface::Params;
use reifydb::core::log_info;
use reifydb::{sync, SessionSync};

fn main() {
	// Create and start an in-memory database with logging
	let mut db = sync::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Create a schema to organize our tables
	log_info!("Creating schema...");
	log_info!("Command: \x1b[1mcreate schema company\x1b[0m");
	db.command_as_root(
		r#"
		create schema company;
		"#,
		Params::None,
	)
	.unwrap();

	// Create a table with various data types
	log_info!("Creating employees table...");
	log_info!("Command: \x1b[1mcreate table company.employees {{\x1b[0m");
	log_info!("\x1b[1m    id: int4,\x1b[0m");
	log_info!("\x1b[1m    name: utf8,\x1b[0m");
	log_info!("\x1b[1m    age: int1,\x1b[0m");
	log_info!("\x1b[1m    salary: float8,\x1b[0m");
	log_info!("\x1b[1m    is_active: bool,\x1b[0m");
	log_info!("\x1b[1m    department: utf8\x1b[0m");
	log_info!("\x1b[1m}}\x1b[0m");
	db.command_as_root(
		r#"
		create table company.employees {
			id: int4,
			name: utf8,
			age: int1,
			salary: float8,
			is_active: bool,
			department: utf8
		};
		"#,
		Params::None,
	)
	.unwrap();

	// Insert some initial data
	log_info!("Inserting employees...");
	log_info!("Command: \x1b[1mfrom [\x1b[0m");
	log_info!("\x1b[1m    {{ id: 1, name: \"Alice Johnson\", age: 28, salary: 75000.0, is_active: true, department: \"Engineering\" }},\x1b[0m");
	log_info!("\x1b[1m    {{ id: 2, name: \"Bob Smith\", age: 35, salary: 85000.0, is_active: true, department: \"Sales\" }},\x1b[0m");
	log_info!("\x1b[1m    {{ id: 3, name: \"Charlie Bframen\", age: 42, salary: 95000.0, is_active: true, department: \"Engineering\" }},\x1b[0m");
	log_info!("\x1b[1m    {{ id: 4, name: \"Diana Prince\", age: 31, salary: 72000.0, is_active: false, department: \"HR\" }},\x1b[0m");
	log_info!("\x1b[1m    {{ id: 5, name: \"Eve Adams\", age: 26, salary: 68000.0, is_active: true, department: \"Marketing\" }}\x1b[0m");
	log_info!("\x1b[1m]\x1b[0m");
	log_info!("\x1b[1minsert company.employees\x1b[0m");
	db.command_as_root(
		r#"
		from [
			{ id: 1, name: "Alice Johnson", age: 28, salary: 75000.0, is_active: true, department: "Engineering" },
			{ id: 2, name: "Bob Smith", age: 35, salary: 85000.0, is_active: true, department: "Sales" },
			{ id: 3, name: "Charlie Bframen", age: 42, salary: 95000.0, is_active: true, department: "Engineering" },
			{ id: 4, name: "Diana Prince", age: 31, salary: 72000.0, is_active: false, department: "HR" },
			{ id: 5, name: "Eve Adams", age: 26, salary: 68000.0, is_active: true, department: "Marketing" }
		]
		insert company.employees;
		"#,
		Params::None,
	)
	.unwrap();

	// Query all employees
	log_info!("Query: \x1b[1mfrom company.employees\x1b[0m");
	let results = db
		.query_as_root(
			r#"
			from company.employees
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		log_info!("{}", frame);
	}

	// Query with filter - find active employees in Engineering
	log_info!("Query: \x1b[1mfrom company.employees filter {{ is_active = true and department = \"Engineering\" }}\x1b[0m");
	let results = db
		.query_as_root(
			r#"
			from company.employees
			filter { is_active = true and department = "Engineering" }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		log_info!("{}", frame);
	}

	// Update operation - give everyone in Engineering a raise
	log_info!("Giving Engineering department a 10% raise...");
	log_info!("Command: \x1b[1mfrom company.employees\x1b[0m");
	log_info!("\x1b[1mfilter {{ department = \"Engineering\" }}\x1b[0m");
	log_info!("\x1b[1mmap {{\x1b[0m");
	log_info!("\x1b[1m    id: id,\x1b[0m");
	log_info!("\x1b[1m    name: name,\x1b[0m");
	log_info!("\x1b[1m    age: age,\x1b[0m");
	log_info!("\x1b[1m    salary: salary * 1.1,\x1b[0m");
	log_info!("\x1b[1m    is_active: is_active,\x1b[0m");
	log_info!("\x1b[1m    department: department\x1b[0m");
	log_info!("\x1b[1m}}\x1b[0m");
	log_info!("\x1b[1mupdate company.employees\x1b[0m");
	db.command_as_root(
		r#"
		from company.employees
		filter { department = "Engineering" }
		map { 
			id: id,
			name: name,
			age: age,
			salary: salary * 1.1,
			is_active: is_active,
			department: department
		}
		update company.employees;
		"#,
		Params::None,
	)
	.unwrap();

	// Query to see the updated salaries
	log_info!("Query: \x1b[1mfrom company.employees filter {{ department = \"Engineering\" }}\x1b[0m");
	let results = db
		.query_as_root(
			r#"
			from company.employees
			filter { department = "Engineering" }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		log_info!("{}", frame);
	}

	// Delete operation - remove inactive employees
	log_info!("Removing inactive employees...");
	log_info!("Command: \x1b[1mfrom company.employees\x1b[0m");
	log_info!("\x1b[1mfilter {{ is_active = false }}\x1b[0m");
	log_info!("\x1b[1mdelete company.employees\x1b[0m");
	db.command_as_root(
		r#"
		from company.employees
		filter { is_active = false }
		delete company.employees;
		"#,
		Params::None,
	)
	.unwrap();

	// Final query - show remaining employees
	log_info!("Query: \x1b[1mfrom company.employees\x1b[0m");
	let results = db
		.query_as_root(
			r#"
			from company.employees
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		log_info!("{}", frame);
	}

	// Query with different filter - high earners
	log_info!("Query: \x1b[1mfrom company.employees filter {{ salary > 80000 }}\x1b[0m");
	let results = db
		.query_as_root(
			r#"
			from company.employees
			filter { salary > 80000 }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		log_info!("{}", frame);
	}
}
