// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error,
	fs,
	path::{Component, Path, PathBuf},
	process::Command,
};

const ROOTS: [&str; 3] = ["crates", "bin", "pkg"];

const SKIPPED: [&str; 2] = ["target", "node_modules"];

pub fn all(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
	let mut found = Vec::new();
	for name in ROOTS {
		all_visit(&root.join(name), &mut found)?;
	}
	found.sort();
	Ok(found)
}

fn all_visit(directory: &Path, found: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
	let entries = fs::read_dir(directory).map_err(|error| format!("{}: {error}", directory.display()))?;
	for entry in entries {
		let entry = entry?;
		let path = entry.path();
		if entry.file_type()?.is_dir() {
			all_visit_directory(&path, found)?;
			continue;
		}
		if is_rust(&path) {
			found.push(path);
		}
	}
	Ok(())
}

fn all_visit_directory(directory: &Path, found: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
	if directory.components().next_back().is_some_and(is_skipped) {
		return Ok(());
	}
	all_visit(directory, found)
}

pub fn changed(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
	let mut names = changed_git(root, &["diff", "--name-only", "--diff-filter=d", "@{upstream}"])?;
	names.push_str(&changed_git(root, &["ls-files", "--others", "--exclude-standard"])?);
	let mut found: Vec<PathBuf> = names.lines().filter(|name| changed_is_source(name)).map(|name| root.join(name)).collect();
	found.sort();
	found.dedup();
	Ok(found)
}

fn changed_git(root: &Path, arguments: &[&str]) -> Result<String, Box<dyn Error>> {
	let output = Command::new("git").current_dir(root).args(arguments).output()?;
	if !output.status.success() {
		return Err(format!("git {arguments:?}: {}", String::from_utf8_lossy(&output.stderr)).into());
	}
	Ok(String::from_utf8(output.stdout)?)
}

fn changed_is_source(name: &str) -> bool {
	let path = Path::new(name);
	let rooted = ROOTS.iter().any(|root| path.starts_with(root));
	rooted && is_rust(path) && !path.components().any(is_skipped)
}

fn is_rust(path: &Path) -> bool {
	path.extension().is_some_and(|extension| extension == "rs")
}

fn is_skipped(component: Component) -> bool {
	let name = component.as_os_str().to_string_lossy();
	name.starts_with('.') || SKIPPED.contains(&name.as_ref())
}

#[cfg(test)]
mod tests {
	use super::changed_is_source;

	#[test]
	fn a_changed_file_is_checked_only_inside_the_three_roots() {
		// Vendored and patched code is not ours to restyle, so it must never be read in either mode.
		assert!(changed_is_source("crates/store/src/tier/range/scan.rs"));
		assert!(changed_is_source("bin/tidy/src/main.rs"));
		assert!(changed_is_source("pkg/rust/reifydb/src/lib.rs"));
		assert!(!changed_is_source("vendor/vortex-array/src/lib.rs"));
		assert!(!changed_is_source("patch/datafusion-common/src/lib.rs"));
		assert!(!changed_is_source("build.rs"));
	}

	#[test]
	fn a_changed_file_in_a_skipped_folder_is_not_checked() {
		// The incremental mode must skip exactly what the full walk skips, otherwise the two modes disagree.
		assert!(!changed_is_source("crates/store/target/debug/build/out.rs"));
		assert!(!changed_is_source("pkg/typescript/node_modules/pkg/src/lib.rs"));
		assert!(!changed_is_source("crates/store/.cache/lib.rs"));
	}

	#[test]
	fn a_changed_file_that_is_not_rust_is_not_checked() {
		// A manifest or a readme handed to the parser would stop the run as a parse error.
		assert!(!changed_is_source("crates/store/Cargo.toml"));
		assert!(!changed_is_source("crates/store/readme.md"));
		assert!(!changed_is_source("crates/store/src/rs"));
	}
}
