//! # Comparison Operators Example
//!
//! Demonstrates comparison operators in ReifyDB's RQL:
//! - Equality (==) and inequality (!=)
//! - Less than (<) and less than or equal (<=)
//! - Greater than (>) and greater than or equal (>=)
//! - BETWEEN operator for range checks
//! - Comparisons with different data types
//!
//! Run with: `make rql-comparison` or `cargo run --bin rql-comparison`

use reifydb::{Params, Session, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	// Create and start an in-memory database
	let mut db = embedded::memory().build().unwrap();
	db.start().unwrap();

	// Example 1: Basic comparisons with numbers
	info!("Example 1: Numeric comparisons");
	log_query(
		r#"map {
  10 = 10 as equals_true,
  10 = 5 as equals_false,
  10 != 5 as not_equals_true,
  10 < 20 as less_than,
  10 <= 10 as less_equal,
  10 > 5 as greater_than,
  10 >= 10 as greater_equal
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				10 = 10 as equals_true,
				10 = 5 as equals_false,
				10 != 5 as not_equals_true,
				10 < 20 as less_than,
				10 <= 10 as less_equal,
				10 > 5 as greater_than,
				10 >= 10 as greater_equal
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 2: String comparisons
	info!("\nExample 2: String comparisons");
	log_query(
		r#"map {
  "apple" == "apple" as string_equals,
  "apple" != "banana" as string_not_equals,
  "apple" < "banana" as string_less_than,
  "zebra" > "apple" as string_greater_than
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				"apple" == "apple" as string_equals,
				"apple" != "banana" as string_not_equals,
				"apple" < "banana" as string_less_than,
				"zebra" > "apple" as string_greater_than
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 3: Boolean comparisons
	info!("\nExample 3: Boolean comparisons");
	log_query(
		r#"map {
  true == true as bool_equals_true,
  true == false as bool_equals_false,
  true != false as bool_not_equals
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				true == true as bool_equals_true,
				true == false as bool_equals_false,
				true != false as bool_not_equals
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Set up sample data
	db.command_as_root("create namespace test", Params::None).unwrap();
	db.command_as_root(
		r#"
		create table test.scores {
			id: int4,
			student: utf8,
			subject: utf8,
			score: int2,
			grade: utf8,
			passed: bool
		}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ id: 1, student: "Alice", subject: "Math", score: 95, grade: "A", passed: true },
			{ id: 2, student: "Bob", subject: "Math", score: 78, grade: "C", passed: true },
			{ id: 3, student: "Carol", subject: "Math", score: 88, grade: "B", passed: true },
			{ id: 4, student: "Dave", subject: "Math", score: 65, grade: "D", passed: true },
			{ id: 5, student: "Eve", subject: "Math", score: 92, grade: "A", passed: true },
			{ id: 6, student: "Frank", subject: "Math", score: 55, grade: "F", passed: false },
			{ id: 7, student: "Grace", subject: "Math", score: 100, grade: "A", passed: true },
			{ id: 8, student: "Henry", subject: "Math", score: 73, grade: "C", passed: true }
		]
		insert test.scores
		"#,
		Params::None,
	)
	.unwrap();

	// Example 4: Equality comparisons in filters
	info!("\nExample 4: Filter with equality (exact match)");
	log_query(r#"from test.scores filter grade == "A""#);
	for frame in db
		.query_as_root(
			r#"
			from test.scores
			filter grade == "A"
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 5: Inequality comparisons
	info!("\nExample 5: Filter with inequality (not equal)");
	log_query(r#"from test.scores filter grade != "F""#);
	for frame in db
		.query_as_root(
			r#"
			from test.scores
			filter grade != "F"
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 6: Greater than comparison
	info!("\nExample 6: Filter scores greater than 85");
	log_query(r#"from test.scores filter score > 85"#);
	for frame in db
		.query_as_root(
			r#"
			from test.scores
			filter score > 85
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 7: Less than or equal comparison
	info!("\nExample 7: Filter scores less than or equal to 70");
	log_query(r#"from test.scores filter score <= 70"#);
	for frame in db
		.query_as_root(
			r#"
			from test.scores
			filter score <= 70
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 8: BETWEEN operator
	info!("\nExample 8: Filter scores between 70 and 90 (inclusive)");
	log_query(r#"from test.scores filter score between 70 and 90"#);
	for frame in db
		.query_as_root(
			r#"
			from test.scores
			filter score between 70 and 90
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 9: Comparisons in computed fields
	info!("\nExample 9: Create computed boolean fields");
	log_query(
		r#"from test.scores
map {
  student,
  score,
  score >= 90 as is_excellent,
  score >= 70 as is_passing,
  score < 60 as needs_help
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from test.scores
			map {
				student,
				score,
				score >= 90 as is_excellent,
				score >= 70 as is_passing,
				score < 60 as needs_help
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 10: Chained comparisons
	info!("\nExample 10: Multiple comparisons in filter");
	log_query(
		r#"from test.scores
filter score >= 80 and score < 95"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from test.scores
			filter score >= 80 and score < 95
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}
}
