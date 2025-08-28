// Tests for explain functionality with UPDATE and DELETE operations

#[cfg(test)]
mod tests {
	use crate::{
        ast::parse_str, explain::logical_plan::explain_logical_plans,
        plan::logical::compile_logical,
	};

	// Helper function to compile and explain a query
	fn compile_and_explain(query: &str) -> String {
		let statements = parse_str(query).unwrap();
		let mut plans = Vec::new();
		for statement in statements {
			plans.extend(compile_logical(statement).unwrap());
		}
		explain_logical_plans(&plans).unwrap()
	}

	// ==================== UPDATE Tests ====================

	#[test]
	fn test_explain_update_optional_not_set() {
		// Case 1: Optional table (not set) - will be inferred from
		// pipeline
		let query = "from users filter active = true update";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Update"));
		assert!(explain.contains("target table: <inferred from input>"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Filter"));
		assert!(explain.contains("condition:"));
	}

	#[test]
	fn test_explain_update_optional_but_set() {
		// Case 2: Optional but set - explicit target table with
		// pipeline
		let query = "from users filter active = true update users";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Update"));
		assert!(explain.contains("target table: users"));
		assert!(!explain
			.contains("target table: <inferred from input>"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Filter"));
	}

	#[test]
	fn test_explain_update_required_inline_data() {
		// Case 3: Required - inline data requires explicit target
		let query = r#"from [{id: 1, name: "John"}] update users"#;
		let explain = compile_and_explain(query);

		assert!(explain.contains("Update"));
		assert!(explain.contains("target table: users"));
		assert!(explain.contains("Input Pipeline:"));
	}

	#[test]
	fn test_explain_update_with_schema() {
		// Test UPDATE with schema.table notation
		let query = "from myschema.users filter active = true update myschema.users";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Update"));
		assert!(explain.contains("target table: myschema.users"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Filter"));
	}

	#[test]
	fn test_explain_update_simple_backward_compat() {
		// Simple UPDATE without pipeline (backward compatibility)
		let query = "update users";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Update"));
		assert!(explain.contains("target table: users"));
		assert!(!explain.contains("Input Pipeline:"));
	}

	#[test]
	fn test_explain_update_with_map() {
		// UPDATE with filter and map operations
		let query = "from users filter age > 18 map { name: upper(name) } update";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Update"));
		assert!(explain.contains("target table: <inferred from input>"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Map"));
	}

	// ==================== DELETE Tests ====================

	#[test]
	fn test_explain_delete_optional_not_set() {
		// Case 1: Optional table (not set) - will be inferred from
		// pipeline
		let query = "from users filter age > 100 delete";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Delete"));
		assert!(explain.contains("target table: <inferred from input>"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Filter"));
	}

	#[test]
	fn test_explain_delete_optional_but_set() {
		// Case 2: Optional but set - explicit target table with
		// pipeline
		let query = "from users filter age > 100 delete users";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Delete"));
		assert!(explain.contains("target table: users"));
		assert!(!explain
			.contains("target table: <inferred from input>"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Filter"));
	}

	#[test]
	fn test_explain_delete_required_inline_data() {
		// Case 3: Required - inline data requires explicit target
		let query = r#"from [{id: 1}, {id: 2}] delete users"#;
		let explain = compile_and_explain(query);

		assert!(explain.contains("Delete"));
		assert!(explain.contains("target table: users"));
		assert!(explain.contains("Input Pipeline:"));
	}

	#[test]
	fn test_explain_delete_with_schema() {
		// Test DELETE with schema.table notation
		let query = "from myschema.logs filter timestamp < '2024-01-01' delete myschema.logs";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Delete"));
		assert!(explain.contains("target table: myschema.logs"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Filter"));
	}

	#[test]
	fn test_explain_delete_simple_backward_compat() {
		// Simple DELETE without pipeline (backward compatibility)
		let query = "delete users";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Delete"));
		assert!(explain.contains("target table: users"));
		assert!(!explain.contains("Input Pipeline:"));
	}

	#[test]
	fn test_explain_delete_complex_pipeline() {
		// DELETE with multiple operations in pipeline
		let query = "from logs filter level = 'DEBUG' take 1000 delete";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Delete"));
		assert!(explain.contains("target table: <inferred from input>"));
		assert!(explain.contains("Input Pipeline:"));
		assert!(explain.contains("Take"));
	}

	// ==================== Edge Cases ====================

	#[test]
	fn test_explain_update_no_table_no_input() {
		// UPDATE with neither table nor input (should have None for
		// both)
		let query = "update";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Update"));
		assert!(explain.contains("target table: <inferred from input>"));
		assert!(!explain.contains("Input Pipeline:"));
	}

	#[test]
	fn test_explain_delete_no_table_no_input() {
		// DELETE with neither table nor input (should have None for
		// both)
		let query = "delete";
		let explain = compile_and_explain(query);

		assert!(explain.contains("Delete"));
		assert!(explain.contains("target table: <inferred from input>"));
		assert!(!explain.contains("Input Pipeline:"));
	}
}
