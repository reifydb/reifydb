use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	generate_tests()?;
	Ok(())
}

fn generate_tests() -> Result<(), Box<dyn std::error::Error>> {
	use reifydb_testing::test_generator::{
        add_rerun_if_changed, generate_combined_test_file, TestConfig, TestGenerator,
    };

	let manifest_dir = env::var("CARGO_MANIFEST_DIR")?;

	// Add rerun directives
	add_rerun_if_changed("tests/scripts");
	add_rerun_if_changed("build.rs");

	// Create separate test generators for optimistic and serializable
	let mut generators = Vec::new();

	// Generate optimistic tests
	let mut generator =
		TestGenerator::new(&manifest_dir, "generated_optimistic")?;
	generator.clean()?;

	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/mvcc".to_string(),
		module_name: "mvcc".to_string(),
		test_fn: "test_optimistic".to_string(),
	})?;

	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/all".to_string(),
		module_name: "all".to_string(),
		test_fn: "test_optimistic".to_string(),
	})?;

	generator.generate()?;
	generators.push(generator);

	// Generate serializable tests
	let mut generator =
		TestGenerator::new(&manifest_dir, "generated_serializable")?;
	generator.clean()?;

	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/mvcc".to_string(),
		module_name: "mvcc".to_string(),
		test_fn: "test_serializable".to_string(),
	})?;

	generator.add(TestConfig {
		manifest_dir: manifest_dir.clone(),
		test_dir: "tests/scripts/all".to_string(),
		module_name: "all".to_string(),
		test_fn: "test_serializable".to_string(),
	})?;

	generator.generate()?;
	generators.push(generator);

	// Generate the main generated_tests.rs file automatically from all generators
	generate_combined_test_file(&generators, "generated_tests")?;

	Ok(())
}
