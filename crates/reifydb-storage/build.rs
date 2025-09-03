use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    generate_tests()?;
    Ok(())
}

fn generate_tests() -> Result<(), Box<dyn std::error::Error>> {
    use reifydb_testing::test_generator::{TestGenerator, TestConfig, add_rerun_if_changed};
    
    let manifest_dir = env::var("CARGO_MANIFEST_DIR")?;
    
    // Add rerun directives
    add_rerun_if_changed("tests/scripts");
    add_rerun_if_changed("build.rs");
    
    // Create test generator
    let mut generator = TestGenerator::new(&manifest_dir, "generated")?;
    
    // Clean old generated files
    generator.clean()?;
    
    // Generate tests for unversioned storage - memory and sqlite
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts/unversioned".to_string(),
        module_name: "unversioned_memory".to_string(),
        test_fn: "test_memory".to_string(),
    })?;
    
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts/unversioned".to_string(),
        module_name: "unversioned_sqlite".to_string(),
        test_fn: "test_sqlite".to_string(),
    })?;
    
    // Generate tests for versioned storage - memory and sqlite
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts/versioned".to_string(),
        module_name: "versioned_memory".to_string(),
        test_fn: "test_memory".to_string(),
    })?;
    
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts/versioned".to_string(),
        module_name: "versioned_sqlite".to_string(),
        test_fn: "test_sqlite".to_string(),
    })?;
    
    // Generate tests for CDC - memory and sqlite
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts/cdc".to_string(),
        module_name: "cdc_memory".to_string(),
        test_fn: "test_memory".to_string(),
    })?;
    
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts/cdc".to_string(),
        module_name: "cdc_sqlite".to_string(),
        test_fn: "test_sqlite".to_string(),
    })?;
    
    // Generate main module file
    generator.generate()?;
    
    Ok(())
}