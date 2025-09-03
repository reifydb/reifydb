use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	generate_tests()?;
	Ok(())
}

fn generate_tests() -> Result<(), Box<dyn std::error::Error>> {
	use reifydb_testing::test_generator::{
		TestConfig, TestGenerator, add_rerun_if_changed,
		generate_combined_test_file,
	};

	let manifest_dir = env::var("CARGO_MANIFEST_DIR")?;

	// Check if generated files exist and force rebuild if missing
	let generated_tests_path = std::path::Path::new(&manifest_dir)
		.join("tests")
		.join("generated_tests.rs");
	let generated_dir = std::path::Path::new(&manifest_dir)
		.join("tests")
		.join("generated");

	if !generated_tests_path.exists() || !generated_dir.exists() {
		// Force rebuild by touching build.rs
		println!("cargo:rerun-if-changed=build.rs");
	}

	// Add rerun directives
	add_rerun_if_changed("tests/scripts");
	add_rerun_if_changed("build.rs");

	let mut generators = Vec::new();

	// Create test generator
	let mut generator = TestGenerator::new(&manifest_dir, "generated")?;

	// Clean old generated files
	generator.clean()?;

	// Generate tests for different script directories
	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/tokenize".to_string(),
		module_name: "tokenize".to_string(),
		test_fn: "test_rql".to_string(),
	})?;

	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/ast".to_string(),
		module_name: "ast".to_string(),
		test_fn: "test_rql".to_string(),
	})?;

	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/logical_plan".to_string(),
		module_name: "logical_plan".to_string(),
		test_fn: "test_rql".to_string(),
	})?;

	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/physical_plan".to_string(),
		module_name: "physical_plan".to_string(),
		test_fn: "test_rql".to_string(),
	})?;

	// Generate main module file
	generator.generate()?;

	generators.push(generator);

	// Generate the main generated_tests.rs file automatically from all
	// generators
	generate_combined_test_file(&generators, "generated_tests")?;

	Ok(())
}
