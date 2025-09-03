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
    
    // Generate tests for embedded mode
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts".to_string(),
        module_name: "embedded".to_string(),
        test_fn: "test_embedded".to_string(),
    })?;
    
    // Generate tests for websocket mode
    generator.add(TestConfig {
        manifest_dir: manifest_dir.clone(),
        test_dir: "tests/scripts".to_string(),
        module_name: "ws".to_string(),
        test_fn: "test_ws".to_string(),
    })?;
    
    // Generate main module file
    generator.generate()?;
    
    Ok(())
}