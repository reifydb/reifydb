//! Zero-dependency test file generator for build scripts
//! 
//! This module provides utilities to generate individual test files from
//! directories of test scripts, making each test individually runnable and debuggable.

use std::fs;
use std::path::{Path, PathBuf};

/// Configuration for generating tests from a directory
pub struct TestConfig {
    /// Path to the manifest directory (usually from CARGO_MANIFEST_DIR)
    pub manifest_dir: String,
    /// Path to the test scripts directory (relative to manifest_dir)
    pub test_dir: String,
    /// Name of the module to generate
    pub module_name: String,
    /// Name of the test function to call
    pub test_fn: String,
}

/// Configuration for multi-file tests (e.g., .in and .out pairs)
pub struct MultiFileTestConfig {
    /// Path to the manifest directory
    pub manifest_dir: String,
    /// Path to the test scripts directory
    pub test_dir: String,
    /// Name of the module to generate
    pub module_name: String,
    /// Name of the test function to call
    pub test_fn: String,
    /// File extensions that must exist together (e.g., ["in", "out"])
    pub file_groups: Vec<String>,
}

/// Main test generator
pub struct TestGenerator {
    /// Output directory for generated tests
    output_dir: PathBuf,
    /// Track generated modules for creating mod.rs
    generated_modules: Vec<String>,
    /// The subdirectory name under tests/ (e.g., "generated_optimistic")
    output_subdir: String,
    /// The manifest directory path
    manifest_dir: String,
    /// Track all test functions used in this generator
    test_functions: std::collections::HashSet<String>,
}

impl TestGenerator {
    /// Derive the module name from a test function name
    /// 
    /// For example: "test_optimistic" -> "optimistic"
    fn derive_module_name(test_fn: &str) -> Option<String> {
        if test_fn.starts_with("test_") {
            Some(test_fn.strip_prefix("test_").unwrap().to_string())
        } else {
            None
        }
    }
    
    /// Create a new test generator
    /// 
    /// # Arguments
    /// * `manifest_dir` - The CARGO_MANIFEST_DIR environment variable value
    /// * `output_subdir` - Subdirectory under tests/ for generated files (e.g., "generated")
    pub fn new(manifest_dir: &str, output_subdir: &str) -> std::io::Result<Self> {
        let output_dir = Path::new(manifest_dir)
            .join("tests")
            .join(output_subdir);
        
        // If the target directory doesn't exist or is being rebuilt (cargo clean was run),
        // clean up generated test files
        let target_dir = Path::new(manifest_dir).join("target");
        if !target_dir.exists() && output_dir.exists() {
            // Target was cleaned, so clean generated tests too
            if let Err(e) = fs::remove_dir_all(&output_dir) {
                eprintln!("Warning: Failed to clean generated tests: {}", e);
            }
        }
        
        // Create output directory
        fs::create_dir_all(&output_dir)?;
        
        // Create .gitignore
        let gitignore = output_dir.join(".gitignore");
        if !gitignore.exists() {
            fs::write(&gitignore, "# Auto-generated test files\n*.rs\n**/\n")?;
        }
        
        Ok(Self {
            output_dir,
            generated_modules: Vec::new(),
            output_subdir: output_subdir.to_string(),
            manifest_dir: manifest_dir.to_string(),
            test_functions: std::collections::HashSet::new(),
        })
    }
    
    /// Clean all generated files (except .gitignore)
    pub fn clean(&self) -> std::io::Result<()> {
        if self.output_dir.exists() {
            for entry in fs::read_dir(&self.output_dir)? {
                let entry = entry?;
                let path = entry.path();
                
                // Keep .gitignore
                if path.file_name() == Some(std::ffi::OsStr::new(".gitignore")) {
                    continue;
                }
                
                if path.is_dir() {
                    fs::remove_dir_all(path)?;
                } else if path.extension().map_or(false, |ext| ext == "rs") {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }
    
    /// Generate tests for a single directory with single-path pattern
    /// 
    /// This generates one test per file in the directory
    pub fn add(&mut self, config: TestConfig) -> std::io::Result<()> {
        let full_path = Path::new(&config.manifest_dir).join(&config.test_dir);
        
        if !full_path.exists() {
            eprintln!("Warning: Test directory does not exist: {}", full_path.display());
            return Ok(());
        }
        
        // Collect test files (without extensions)
        let mut test_files = Vec::new();
        self.collect_test_files(&full_path, &mut test_files)?;
        test_files.sort();
        
        if test_files.is_empty() {
            eprintln!("Warning: No test files found in: {}", full_path.display());
            return Ok(());
        }
        
        // Create module directory
        let module_dir = self.output_dir.join(&config.module_name);
        fs::create_dir_all(&module_dir)?;
        
        // Track this module and test function
        self.generated_modules.push(config.module_name.clone());
        self.test_functions.insert(config.test_fn.clone());
        
        // Generate individual test files preserving directory structure
        for file_path in test_files.iter() {
            // Get the relative path from the test_dir
            let relative_from_test_dir = file_path
                .strip_prefix(&full_path)
                .unwrap_or(file_path);
            
            // Create the same directory structure in the output
            let test_file_path = if let Some(parent) = relative_from_test_dir.parent() {
                if !parent.as_os_str().is_empty() {
                    let parent_dir = module_dir.join(parent);
                    fs::create_dir_all(&parent_dir)?;
                    parent_dir.join(format!("{}.rs", relative_from_test_dir.file_name().unwrap().to_string_lossy()))
                } else {
                    module_dir.join(format!("{}.rs", relative_from_test_dir.file_name().unwrap().to_string_lossy()))
                }
            } else {
                module_dir.join(format!("{}.rs", relative_from_test_dir.file_name().unwrap().to_string_lossy()))
            };
            
            // Test function name: replace path separators with underscores
            let test_name = relative_from_test_dir
                .to_string_lossy()
                .replace('/', "_")
                .replace('\\', "_");
            
            let relative_path = file_path
                .strip_prefix(&config.manifest_dir)
                .unwrap_or(file_path)
                .to_string_lossy()
                .replace('\\', "/");
            
            let content = self.format_single_test_file(
                &test_name,
                &relative_path,
                &config.test_fn,
                &config.module_name,
                file_path,
            );
            
            fs::write(test_file_path, content)?;
        }
        
        // Generate mod.rs for this module  
        self.generate_module_file(&module_dir)?;
        
        println!("Generated tests in module '{}'", config.module_name);
        
        Ok(())
    }
    
    /// Generate tests for multi-file patterns (e.g., .in/.out pairs)
    pub fn generate_multi_path_tests(&mut self, config: MultiFileTestConfig) -> std::io::Result<()> {
        let full_path = Path::new(&config.manifest_dir).join(&config.test_dir);
        
        if !full_path.exists() {
            eprintln!("Warning: Test directory does not exist: {}", full_path.display());
            return Ok(());
        }
        
        // Find file groups
        let test_groups = self.find_file_groups(&full_path, &config.file_groups)?;
        
        if test_groups.is_empty() {
            eprintln!("Warning: No matching file groups found in: {}", full_path.display());
            return Ok(());
        }
        
        // Create module directory
        let module_dir = self.output_dir.join(&config.module_name);
        fs::create_dir_all(&module_dir)?;
        
        // Track this module and test function  
        self.generated_modules.push(config.module_name.clone());
        self.test_functions.insert(config.test_fn.clone());
        
        // Generate test files preserving directory structure
        for group in test_groups.iter() {
            // Get relative path from test_dir for the first file
            let relative_from_test_dir = group[0]
                .strip_prefix(&full_path)
                .unwrap_or(&group[0]);
            
            // Create the same directory structure in the output
            let stem = relative_from_test_dir.with_extension("");
            let test_file_path = if let Some(parent) = stem.parent() {
                if !parent.as_os_str().is_empty() {
                    let parent_dir = module_dir.join(parent);
                    fs::create_dir_all(&parent_dir)?;
                    parent_dir.join(format!("{}.rs", stem.file_name().unwrap().to_string_lossy()))
                } else {
                    module_dir.join(format!("{}.rs", stem.file_name().unwrap().to_string_lossy()))
                }
            } else {
                module_dir.join(format!("{}.rs", stem.file_name().unwrap().to_string_lossy()))
            };
            
            let test_name = stem
                .to_string_lossy()
                .replace('/', "_")
                .replace('\\', "_");
            
            let relative_paths: Vec<String> = group.iter()
                .map(|p| p.strip_prefix(&config.manifest_dir)
                    .unwrap_or(p)
                    .to_string_lossy()
                    .replace('\\', "/"))
                .collect();
            
            let content = self.format_multi_test_file(
                &test_name,
                &relative_paths,
                &config.test_fn,
                &config.module_name,
                config.file_groups.len(),
            );
            
            fs::write(test_file_path, content)?;
        }
        
        // Generate mod.rs for this module
        self.generate_module_file(&module_dir)?;
        
        println!("Generated tests in module '{}'", config.module_name);
        
        Ok(())
    }
    
    /// Generate the main mod.rs file that includes all modules
    pub fn generate(&self) -> std::io::Result<()> {
        let mut content = String::new();
        content.push_str(
r#"//! Auto-generated test modules
//! 
//! This file is generated by build.rs - DO NOT EDIT
//! 
//! To run all generated tests: cargo test generated::
//! To run a specific module: cargo test generated::MODULE_NAME::
//! To run a specific test: cargo test generated::MODULE_NAME::TEST_NAME

"#);
        
        let mut modules = self.generated_modules.clone();
        modules.sort();
        modules.dedup();
        
        for module in modules {
            content.push_str(&format!("pub mod {};\n", module));
        }
        
        let mod_file = self.output_dir.join("mod.rs");
        fs::write(mod_file, content)?;
        
        Ok(())
    }
    
    // ===== Private helper methods =====
    
    fn collect_test_files(
        &self,
        dir: &Path,
        files: &mut Vec<PathBuf>
    ) -> std::io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                // Recursively collect from subdirectories
                self.collect_test_files(&path, files)?;
            } else if path.is_file() {
                // Only include files WITHOUT extensions (test files don't have extensions)
                if path.extension().is_none() {
                    files.push(path);
                }
            }
        }
        Ok(())
    }
    
    fn find_file_groups(
        &self,
        dir: &Path,
        extensions: &[String]
    ) -> std::io::Result<Vec<Vec<PathBuf>>> {
        // First, find all files with the first extension
        let first_ext = &extensions[0];
        let mut base_files = Vec::new();
        self.collect_files_with_extension(dir, first_ext, &mut base_files)?;
        
        let mut groups = Vec::new();
        
        for base_file in base_files {
            let stem = base_file.with_extension("");
            let mut group = vec![base_file.clone()];
            let mut all_exist = true;
            
            // Check for other extensions
            for ext in &extensions[1..] {
                let other_file = stem.with_extension(ext);
                if other_file.exists() {
                    group.push(other_file);
                } else {
                    all_exist = false;
                    break;
                }
            }
            
            if all_exist && group.len() == extensions.len() {
                groups.push(group);
            }
        }
        
        groups.sort();
        Ok(groups)
    }
    
    fn collect_files_with_extension(
        &self,
        dir: &Path,
        extension: &str,
        files: &mut Vec<PathBuf>
    ) -> std::io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                self.collect_files_with_extension(&path, extension, files)?;
            } else if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == extension {
                        files.push(path);
                    }
                }
            }
        }
        Ok(())
    }
    
    fn format_single_test_file(
        &self,
        test_name: &str,
        relative_path: &str,
        test_fn: &str,
        module_name: &str,
        original_path: &Path,
    ) -> String {
        let file_name = original_path.file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();
        
        // Calculate the depth to determine the number of super:: needed
        // Count slashes after "tests/scripts/" to determine nesting level
        let after_scripts = relative_path.find("scripts/").map(|i| &relative_path[i+8..]).unwrap_or(relative_path);
        let depth = after_scripts.matches('/').count() + 2; // +2 for module dir and generated dir
        let super_path = "super::".repeat(depth);
        
        format!(
r#"//! Auto-generated test for: {relative_path}
//! File: {file_name}
//! 
//! This file is auto-generated by build.rs - DO NOT EDIT
//! 
//! To run this specific test:
//!   cargo test generated::{module_name}::{test_name}
//! 
//! To debug in IDE:
//!   Set a breakpoint in this file and run the test

#[test]
fn {test_name}() {{
    // Import the test function from the parent test file
    use {super_path}{test_fn};
    
    let path = ::std::path::Path::new("{relative_path}");
    {test_fn}(path);
}}
"#)
    }
    
    fn format_multi_test_file(
        &self,
        test_name: &str,
        relative_paths: &[String],
        test_fn: &str,
        module_name: &str,
        array_size: usize,
    ) -> String {
        let paths_list = relative_paths.join(", ");
        let paths_array = relative_paths.iter()
            .map(|p| format!("        ::std::path::Path::new(\"{}\"),", p))
            .collect::<Vec<_>>()
            .join("\n");
        
        // Calculate the depth to determine the number of super:: needed  
        let depth = relative_paths.get(0).map_or(3, |p| {
            let after_scripts = p.find("scripts/").map(|i| &p[i+8..]).unwrap_or(p);
            after_scripts.matches('/').count() + 2
        });
        let super_path = "super::".repeat(depth);
        
        format!(
r#"//! Auto-generated test for file group
//! Files: {paths_list}
//! 
//! This file is auto-generated by build.rs - DO NOT EDIT
//! 
//! To run this specific test:
//!   cargo test generated::{module_name}::{test_name}

#[test]
fn {test_name}() {{
    use {super_path}{test_fn};
    
    let paths: [&::std::path::Path; {array_size}] = [
{paths_array}
    ];
    
    {test_fn}(paths);
}}
"#)
    }
    
    fn generate_module_file(&self, module_dir: &Path) -> std::io::Result<()> {
        // Recursively find all .rs files in the module directory and generate mod declarations
        let mut mod_content = String::new();
        mod_content.push_str(
r#"//! Auto-generated test module
//! 
//! This file is generated by build.rs - DO NOT EDIT

"#);
        
        self.generate_mod_declarations(module_dir, module_dir, &mut mod_content)?;
        
        let mod_file = module_dir.join("mod.rs");
        fs::write(mod_file, mod_content)?;
        
        Ok(())
    }
    
    fn generate_mod_declarations(&self, base_dir: &Path, current_dir: &Path, content: &mut String) -> std::io::Result<()> {
        let mut entries = Vec::new();
        
        // Collect all entries
        for entry in fs::read_dir(current_dir)? {
            let entry = entry?;
            entries.push(entry.path());
        }
        
        entries.sort();
        
        // Process directories first (as submodules)
        for path in &entries {
            if path.is_dir() {
                let dir_name = path.file_name().unwrap().to_string_lossy();
                content.push_str(&format!("pub mod {};\n", dir_name));
            }
        }
        
        // Then process .rs files
        for path in &entries {
            if path.is_file() && path.extension().map_or(false, |ext| ext == "rs") {
                let file_name = path.file_stem().unwrap().to_string_lossy();
                if file_name != "mod" {
                    // Ensure module name is valid - prefix with 't_' if it starts with a number
                    let module_name = if file_name.chars().next().map_or(false, |c| c.is_numeric()) {
                        format!("t_{}", file_name)
                    } else {
                        file_name.to_string()
                    };
                    content.push_str(&format!("#[path = \"{}.rs\"]\n", file_name));
                    content.push_str(&format!("mod {};\n", module_name));
                }
            }
        }
        
        // Recursively generate mod.rs for subdirectories
        for path in &entries {
            if path.is_dir() {
                let mut submod_content = String::new();
                submod_content.push_str(
r#"//! Auto-generated test submodule
//! 
//! This file is generated by build.rs - DO NOT EDIT

"#);
                self.generate_mod_declarations(base_dir, path, &mut submod_content)?;
                let submod_file = path.join("mod.rs");
                fs::write(submod_file, submod_content)?;
            }
        }
        
        Ok(())
    }
}

/// Helper to add cargo rerun-if-changed directives
pub fn add_rerun_if_changed(path: &str) {
    println!("cargo:rerun-if-changed={}", path);
}

/// Generate a combined test file from multiple TestGenerator instances
/// 
/// This automatically derives all necessary imports and includes all generated test modules.
pub fn generate_combined_test_file(
    generators: &[TestGenerator],
    output_name: &str,
) -> std::io::Result<()> {
    if generators.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "No generators provided",
        ));
    }
    
    // Use the manifest_dir from the first generator (they should all be the same)
    let manifest_dir = &generators[0].manifest_dir;
    let output_path = Path::new(manifest_dir)
        .join("tests")
        .join(format!("{}.rs", output_name));
    
    // If target directory doesn't exist (cargo clean was run), remove the generated test file
    let target_dir = Path::new(manifest_dir).join("target");
    if !target_dir.exists() && output_path.exists() {
        if let Err(e) = fs::remove_file(&output_path) {
            eprintln!("Warning: Failed to clean generated test file: {}", e);
        }
    }
    
    let mut content = String::new();
    
    // Add header
    content.push_str(
r#"// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Auto-generated test modules
//! 
//! This file is generated by build.rs - DO NOT EDIT
//! 
//! This file includes the auto-generated test modules created by the build script.
//! The tests are generated from the scripts in tests/scripts/ directory.

"#);
    
    // Collect all test functions and their derived module names
    let mut test_fn_to_module = std::collections::HashMap::new();
    let mut all_test_functions = std::collections::HashSet::new();
    
    for generator in generators {
        for test_fn in &generator.test_functions {
            all_test_functions.insert(test_fn.clone());
            if let Some(module_name) = TestGenerator::derive_module_name(test_fn) {
                test_fn_to_module.insert(test_fn.clone(), module_name);
            }
        }
    }
    
    // Generate imports for test functions
    if !all_test_functions.is_empty() {
        content.push_str("// Import the test functions from the other test files\n");
        content.push_str("// These are used by the generated tests\n");
        
        // Sort for consistent output
        let mut sorted_functions: Vec<_> = all_test_functions.iter().collect();
        sorted_functions.sort();
        
        for test_fn in sorted_functions {
            if let Some(module_name) = test_fn_to_module.get(test_fn) {
                content.push_str(&format!("pub use {}::{};\n", module_name, test_fn));
            }
        }
        content.push_str("\n");
    }
    
    // Import the source modules
    let mut source_modules = std::collections::HashSet::new();
    for module_name in test_fn_to_module.values() {
        source_modules.insert(module_name.clone());
    }
    
    if !source_modules.is_empty() {
        content.push_str("// Import the other test modules to access their public functions\n");
        
        // Sort for consistent output
        let mut sorted_modules: Vec<_> = source_modules.iter().collect();
        sorted_modules.sort();
        
        for module_name in sorted_modules {
            content.push_str(&format!("mod {};\n", module_name));
        }
        content.push_str("\n");
    }
    
    // Include all generated test module directories
    content.push_str("// Include the generated test modules\n");
    for generator in generators {
        content.push_str(&format!("pub mod {};\n", generator.output_subdir));
    }
    
    fs::write(&output_path, content)?;
    
    println!("cargo:rerun-if-changed={}", output_path.display());
    
    Ok(())
}

