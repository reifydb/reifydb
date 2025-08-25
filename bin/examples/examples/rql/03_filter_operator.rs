//! # FILTER Operator Example
//!
//! Demonstrates the FILTER operator in ReifyDB's RQL:
//! - Basic comparisons (==, !=, <, >, <=, >=)
//! - Logical operators (and, or, not)
//! - Filtering different data types
//! - Comptokenize filter conditions
//!
//! Run with: `make rql-filter` or `cargo run --bin rql-filter`

use reifydb::{log_info, sync, Params, SessionSync};
use reifydb_examples::log_query;

fn main() {
	// Create and start an in-memory database
	let mut db = sync::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Set up sample data
	log_info!("Setting up sample employee data...");
	db.command_as_root("create schema hr", Params::None).unwrap();
	db.command_as_root(
		r#"
		create table hr.employees {
			id: int4,
			name: utf8,
			department: utf8,
			salary: int4,
			years_experience: int2,
			is_manager: bool
		}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ id: 1, name: "Alice", department: "Engineering", salary: 120000, years_experience: 8, is_manager: true },
			{ id: 2, name: "Bob", department: "Sales", salary: 80000, years_experience: 5, is_manager: false },
			{ id: 3, name: "Carol", department: "Engineering", salary: 95000, years_experience: 3, is_manager: false },
			{ id: 4, name: "Dave", department: "HR", salary: 75000, years_experience: 4, is_manager: false },
			{ id: 5, name: "Eve", department: "Sales", salary: 110000, years_experience: 7, is_manager: true },
			{ id: 6, name: "Frank", department: "Engineering", salary: 105000, years_experience: 6, is_manager: false },
			{ id: 7, name: "Grace", department: "Marketing", salary: 90000, years_experience: 5, is_manager: true },
			{ id: 8, name: "Henry", department: "Engineering", salary: 130000, years_experience: 10, is_manager: true }
		]
		insert hr.employees
		"#,
		Params::None,
	)
	.unwrap();

	// Example 1: Simple equality filter
	log_info!("\nExample 1: Filter by exact match (equality)");
	log_query(r#"from hr.employees filter department == "Engineering""#);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter department == "Engineering"
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows only Engineering department employees
	}

	// Example 2: Not equal filter
	log_info!("\nExample 2: Filter by not equal");
	log_query(r#"from hr.employees filter department != "Engineering""#);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter department != "Engineering"
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows all non-Engineering employees
	}

	// Example 3: Greater than filter
	log_info!("\nExample 3: Filter by greater than");
	log_query(r#"from hr.employees filter salary > 100000"#);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter salary > 100000
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows employees with salary > 100000
	}

	// Example 4: Less than or equal filter
	log_info!("\nExample 4: Filter by less than or equal");
	log_query(r#"from hr.employees filter years_experience <= 5"#);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter years_experience <= 5
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows employees with 5 or fewer years experience
	}

	// Example 5: Boolean filter
	log_info!("\nExample 5: Filter by boolean value");
	log_query(r#"from hr.employees filter is_manager == true"#);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter is_manager == true
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows only managers
	}

	// Example 6: AND operator
	log_info!("\nExample 6: Filter with AND operator");
	log_query(
		r#"from hr.employees
filter department == "Engineering" and salary > 100000"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter department == "Engineering" and salary > 100000
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows Engineering employees earning > 100000
	}

	// Example 7: OR operator
	log_info!("\nExample 7: Filter with OR operator");
	log_query(
		r#"from hr.employees
filter department == "Sales" or department == "Marketing""#,
	);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter department == "Sales" or department == "Marketing"
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows Sales or Marketing employees
	}

	// Example 8: Comptokenize filter with parentheses
	log_info!("\nExample 8: Comptokenize filter with parentheses");
	log_query(
		r#"from hr.employees
filter (department == "Engineering" or department == "Sales")
   and salary >= 100000"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter (department == "Engineering" or department == "Sales") 
			   and salary >= 100000
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows Engineering or Sales employees earning >= 100000
	}

	// Example 9: Filter on inline data
	log_info!("\nExample 9: Filter on inline data");
	log_query(
		r#"from [
  { score: 85, grade: "B" },
  { score: 92, grade: "A" },
  { score: 78, grade: "C" },
  { score: 95, grade: "A" }
]
filter score >= 90"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from [
				{ score: 85, grade: "B" },
				{ score: 92, grade: "A" },
				{ score: 78, grade: "C" },
				{ score: 95, grade: "A" }
			]
			filter score >= 90
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows only scores >= 90
	}

	// Example 10: Multiple filters in sequence
	log_info!("\nExample 10: Multiple filters in sequence");
	log_query(
		r#"from hr.employees
filter salary > 80000
filter is_manager == false"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter salary > 80000
			filter is_manager == false
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows non-managers earning > 80000
	}

	// Example 11: BETWEEN operator
	log_info!("\nExample 11: Filter with BETWEEN operator");
	log_query(
		r#"from hr.employees
filter salary between 90000 and 110000"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from hr.employees
			filter salary between 90000 and 110000
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Shows employees with salary between 90000 and 110000
		// (inclusive)
	}
}
