// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Test to demonstrate colored diff output and ensure goldenfile behavior
use std::{fs, io::Write};

use reifydb_testing::goldenfile::{self, Mode};

#[test]
#[should_panic(expected = "Golden file test failed")]
fn test_colored_diff_output() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_test_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// First, create a golden file
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("test.txt").unwrap();
		writeln!(file, "Line 1: This is the original content").unwrap();
		writeln!(file, "Line 2: Everything is fine").unwrap();
		writeln!(file, "Line 3: No changes here").unwrap();
		writeln!(file, "Line 4: All good").unwrap();
		writeln!(file, "Line 5: The end").unwrap();
	}

	// Now test with different content to trigger the diff
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("test.txt").unwrap();
		writeln!(file, "Line 1: This is MODIFIED content").unwrap();
		writeln!(file, "Line 2: Everything is fine").unwrap();
		writeln!(file, "Line 3: This line was changed").unwrap();
		writeln!(file, "Line 4: All good").unwrap();
		writeln!(file, "Line 5: Different ending").unwrap();
		writeln!(file, "Line 6: Added a new line").unwrap();
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_goldenfile_success() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_success_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Create and verify identical content
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("success.txt").unwrap();
		writeln!(file, "Matching content").unwrap();
	}

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("success.txt").unwrap();
		writeln!(file, "Matching content").unwrap();
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_update_testfiles_env_var() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_env_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Test explicit update mode
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("env_test.txt").unwrap();
		writeln!(file, "Initial content").unwrap();
	}

	// Verify the file was created
	assert!(test_dir.join("env_test.txt").exists());

	// Now update with different content
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("env_test.txt").unwrap();
		writeln!(file, "Updated content").unwrap();
	}

	// Verify the file was updated
	let content = fs::read_to_string(test_dir.join("env_test.txt")).unwrap();
	assert_eq!(content, "Updated content\n");
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_update_goldenfiles_env_var() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_env2_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Test explicit update mode (alternative test)
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("env_test2.txt").unwrap();
		writeln!(file, "Content via explicit mode").unwrap();
	}

	// Verify the file was created
	assert!(test_dir.join("env_test2.txt").exists());
	let content = fs::read_to_string(test_dir.join("env_test2.txt")).unwrap();
	assert_eq!(content, "Content via explicit mode\n");
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
#[should_panic(expected = "does not exist")]
fn test_missing_golden_file() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_missing_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Try to verify against a non-existent golden file
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("missing.txt").unwrap();
		writeln!(file, "This will fail").unwrap();
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_new_goldenfile_alias() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_alias_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Test that new_golden_file is an alias for new_goldenfile
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);

		// Use the alias method
		let mut file = mint.new_golden_file("alias_test.txt").unwrap();
		writeln!(file, "Testing alias").unwrap();
	}

	// Verify the file was created
	assert!(test_dir.join("alias_test.txt").exists());
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_nested_directories() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_nested_{}", std::process::id()));

	// Test creating golden files in nested directories
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("deeply/nested/dir/file.txt").unwrap();
		writeln!(file, "Nested file content").unwrap();
	}

	// Verify the nested file was created
	assert!(test_dir.join("deeply/nested/dir/file.txt").exists());
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
#[should_panic(expected = "0035")] // Should show line number in diff
fn test_diff_shows_line_numbers() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_linenum_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Create a file with many lines
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("lines.txt").unwrap();
		for i in 1..=40 {
			writeln!(file, "Line {}", i).unwrap();
		}
	}

	// Change line 35
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("lines.txt").unwrap();
		for i in 1..=40 {
			if i == 35 {
				writeln!(file, "Line {} CHANGED", i).unwrap();
			} else {
				writeln!(file, "Line {}", i).unwrap();
			}
		}
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_empty_files() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_empty_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Create an empty golden file
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let _file = mint.new_goldenfile("empty.txt").unwrap();
		// Don't write anything
	}

	// Verify against empty content
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let _file = mint.new_goldenfile("empty.txt").unwrap();
		// Don't write anything - should pass
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
#[should_panic(expected = "Golden file test failed")]
fn test_empty_vs_content() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_empty_vs_content_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Create an empty golden file
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let _file = mint.new_goldenfile("empty2.txt").unwrap();
		// Don't write anything
	}

	// Try to verify with content - should fail
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("empty2.txt").unwrap();
		writeln!(file, "Some content").unwrap();
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_multiple_files_same_mint() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_multi_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Create multiple files with the same Mint instance
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);

		let mut file1 = mint.new_goldenfile("file1.txt").unwrap();
		writeln!(file1, "File 1 content").unwrap();

		let mut file2 = mint.new_goldenfile("file2.txt").unwrap();
		writeln!(file2, "File 2 content").unwrap();

		let mut file3 = mint.new_goldenfile("subdir/file3.txt").unwrap();
		writeln!(file3, "File 3 content").unwrap();
	}

	// Verify all files were created
	assert!(test_dir.join("file1.txt").exists());
	assert!(test_dir.join("file2.txt").exists());
	assert!(test_dir.join("subdir/file3.txt").exists());
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_long_lines_truncation() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_long_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	let long_line = "x".repeat(150); // Create a line longer than 100 chars

	// Create golden file with long line
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("long.txt").unwrap();
		writeln!(file, "{}", long_line).unwrap();
	}

	// Verify with same content - should pass
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("long.txt").unwrap();
		writeln!(file, "{}", long_line).unwrap();
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_concurrent_update_never_empties_file() {
	use std::{
		sync::{
			Arc,
			atomic::{AtomicBool, AtomicUsize, Ordering},
		},
		thread,
	};

	// Regression for the UPDATE_TESTFILES race: the same golden file is rewritten in update
	// mode by multiple test entry points (e.g. memory + sqlite backends) running in parallel.
	// The pre-fix code truncated the golden file in place at open time, so a concurrent reader
	// could observe it momentarily empty. With the atomic temp+rename write, the golden path is
	// only ever replaced atomically, so any successful read must yield the complete content -
	// never empty, never partial. A non-zero violation count means a truncated file leaked.
	let test_dir = std::env::temp_dir().join(format!("goldenfile_race_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();
	let path = test_dir.join("race.txt");

	let expected: String = (1..=50).map(|i| format!("line {i}\n")).collect();

	let writers_done = Arc::new(AtomicBool::new(false));
	let violations = Arc::new(AtomicUsize::new(0));

	let mut reader_handles = Vec::new();
	for _ in 0..4 {
		let path = path.clone();
		let expected = expected.clone();
		let writers_done = writers_done.clone();
		let violations = violations.clone();
		reader_handles.push(thread::spawn(move || {
			while !writers_done.load(Ordering::Relaxed) {
				if let Ok(content) = fs::read_to_string(&path)
					&& content != expected
				{
					violations.fetch_add(1, Ordering::Relaxed);
				}
			}
		}));
	}

	let mut writer_handles = Vec::new();
	for _ in 0..8 {
		let test_dir = test_dir.clone();
		let expected = expected.clone();
		writer_handles.push(thread::spawn(move || {
			for _ in 0..200 {
				let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
				let mut file = mint.new_goldenfile("race.txt").unwrap();
				file.write_all(expected.as_bytes()).unwrap();
			}
		}));
	}

	for h in writer_handles {
		h.join().unwrap();
	}
	writers_done.store(true, Ordering::Relaxed);
	for h in reader_handles {
		h.join().unwrap();
	}

	assert_eq!(
		violations.load(Ordering::Relaxed),
		0,
		"a reader observed an empty or partial golden file during concurrent update"
	);
	assert_eq!(fs::read_to_string(&path).unwrap(), expected);
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_binary_safety() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_binary_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Test with non-UTF8 sequences (but valid as bytes)
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("binary.txt").unwrap();
		// Write some bytes that form valid UTF-8
		file.write_all(b"Hello\nWorld\n").unwrap();
		file.write_all(&[0xE2, 0x98, 0x83]).unwrap(); // ☃ (snowman)
		file.write_all(b"\n").unwrap();
	}

	// Verify with same content
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("binary.txt").unwrap();
		file.write_all(b"Hello\nWorld\n").unwrap();
		file.write_all(&[0xE2, 0x98, 0x83]).unwrap(); // ☃ (snowman)
		file.write_all(b"\n").unwrap();
	}

	// Clean up
	let _ = fs::remove_dir_all(&test_dir);
}
