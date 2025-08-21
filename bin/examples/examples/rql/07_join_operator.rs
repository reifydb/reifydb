//! # JOIN Operator Example
//!
//! Demonstrates the JOIN operator in ReifyDB's RQL:
//! - Inner joins
//! - Left joins
//! - Natural joins
//! - Join conditions
//!
//! Run with: `make rql-join` or `cargo run --bin rql-join`

use reifydb::{log_info, sync, Params, SessionSync};

/// Helper function to log queries with formatting
/// The query text is displayed in bold for better readability
fn log_query(query: &str) {
	log_info!("Query:");
	// Split the query into lines and format each line with bold
	let formatted_query = query
		.lines()
		.map(|line| format!("\x1b[1m{}\x1b[0m", line))
		.collect::<Vec<_>>()
		.join("\n");
	log_info!("{}", formatted_query);
}

fn main() {
	// Create and start an in-memory database
	let mut db = sync::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Set up sample data with relationships
	db.command_as_root("create schema company", Params::None).unwrap();

	// Create employees table
	db.command_as_root(
		r#"
		create table company.employees {
			emp_id: int4,
			name: utf8,
			dept_id: int4,
			salary: int4
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Create departments table
	db.command_as_root(
		r#"
		create table company.departments {
			dept_id: int4,
			dept_name: utf8,
			location: utf8
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Create projects table
	db.command_as_root(
		r#"
		create table company.projects {
			project_id: int4,
			project_name: utf8,
			dept_id: int4,
			budget: int4
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Insert data
	db.command_as_root(
		r#"
		from [
			{ emp_id: 1, name: "Alice", dept_id: 10, salary: 75000 },
			{ emp_id: 2, name: "Bob", dept_id: 20, salary: 65000 },
			{ emp_id: 3, name: "Carol", dept_id: 10, salary: 80000 },
			{ emp_id: 4, name: "Dave", dept_id: 30, salary: 70000 },
			{ emp_id: 5, name: "Eve", dept_id: 20, salary: 72000 },
			{ emp_id: 6, name: "Frank", dept_id: 40, salary: 68000 }
		]
		insert company.employees
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ dept_id: 10, dept_name: "Engineering", location: "Building A" },
			{ dept_id: 20, dept_name: "Sales", location: "Building B" },
			{ dept_id: 30, dept_name: "Marketing", location: "Building C" }
		]
		insert company.departments
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ project_id: 1, project_name: "Project Alpha", dept_id: 10, budget: 100000 },
			{ project_id: 2, project_name: "Project Beta", dept_id: 20, budget: 50000 },
			{ project_id: 3, project_name: "Project Gamma", dept_id: 10, budget: 75000 },
			{ project_id: 4, project_name: "Project Delta", dept_id: 30, budget: 60000 }
		]
		insert company.projects
		"#,
		Params::None,
	)
	.unwrap();

	// Example 1: Inner join
	log_info!("Example 1: Inner join employees with departments");
	log_query(
		r#"from company.employees
inner join {
  with company.departments
  on employees.dept_id == departments.dept_id
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from company.employees
			inner join {
				with company.departments
				on employees.dept_id == departments.dept_id
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 2: Left join (includes all employees, even without
	// department)
	log_info!("\nExample 2: Left join employees with departments");
	log_query(
		r#"from company.employees
left join {
  with company.departments
  on employees.dept_id == departments.dept_id
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from company.employees
			left join {
				with company.departments
				on employees.dept_id == departments.dept_id
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 3: Natural join (joins on common column name)
	log_info!("\nExample 3: Natural join (automatic on dept_id)");
	log_query(
		r#"from company.employees
natural join { with company.departments }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from company.employees
			natural join { with company.departments }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 4: Join with filter
	log_info!("\nExample 4: Join then filter for specific location");
	log_query(
		r#"from company.employees
inner join {
  with company.departments
  on employees.dept_id == departments.dept_id
}
filter departments.location == "Building A""#,
	);
	for frame in db
		.query_as_root(
			r#"
			from company.employees
			inner join {
				with company.departments
				on employees.dept_id == departments.dept_id
			}
			filter departments.location == "Building A"
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 5: Join with projection
	log_info!("\nExample 5: Join and select specific columns");
	log_query(
		r#"from company.employees
inner join {
  with company.departments
  on employees.dept_id == departments.dept_id
}
map { employees.name, departments.dept_name, employees.salary }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from company.employees
			inner join {
				with company.departments
				on employees.dept_id == departments.dept_id
			}
			map { employees.name, departments.dept_name, employees.salary }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 6: Multiple joins
	log_info!("\nExample 6: Join employees with departments and projects");
	log_query(
		r#"from company.employees
inner join {
  with company.departments
  on employees.dept_id == departments.dept_id
}
inner join {
  with company.projects
  on departments.dept_id == projects.dept_id
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from company.employees
			inner join {
				with company.departments
				on employees.dept_id == departments.dept_id
			}
			inner join {
				with company.projects
				on departments.dept_id == projects.dept_id
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 7: Join with aggregation
	log_info!("\nExample 7: Join and aggregate - average salary by department");
	log_query(
		r#"from company.employees
inner join {
  with company.departments
  on employees.dept_id == departments.dept_id
}
aggregate { avg(employees.salary), count(employees.emp_id) }
  by departments.dept_name"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from company.employees
			inner join {
				with company.departments
				on employees.dept_id == departments.dept_id
			}
			aggregate { avg(employees.salary), count(employees.emp_id) }
				by departments.dept_name
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}
}
