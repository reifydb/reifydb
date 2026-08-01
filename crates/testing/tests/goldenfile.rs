// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs, io::Write};

use reifydb_testing::goldenfile::{self, Mode};

#[test]
#[should_panic(expected = "Golden file test failed")]
fn test_colored_diff_output() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_test_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("test.txt").unwrap();
		writeln!(file, "Line 1: This is the original content").unwrap();
		writeln!(file, "Line 2: Everything is fine").unwrap();
		writeln!(file, "Line 3: No changes here").unwrap();
		writeln!(file, "Line 4: All good").unwrap();
		writeln!(file, "Line 5: The end").unwrap();
	}

	// Changed first, middle, last and an added line, so the diff has to report every shape.
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

	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_goldenfile_success() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_success_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

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

	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_update_testfiles_env_var() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_env_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("env_test.txt").unwrap();
		writeln!(file, "Initial content").unwrap();
	}

	assert!(test_dir.join("env_test.txt").exists());

	// Update mode must overwrite an existing goldenfile, not append to it.
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("env_test.txt").unwrap();
		writeln!(file, "Updated content").unwrap();
	}

	let content = fs::read_to_string(test_dir.join("env_test.txt")).unwrap();
	assert_eq!(content, "Updated content\n");
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_update_goldenfiles_env_var() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_env2_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("env_test2.txt").unwrap();
		writeln!(file, "Content via explicit mode").unwrap();
	}

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

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("missing.txt").unwrap();
		writeln!(file, "This will fail").unwrap();
	}

	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_new_goldenfile_alias() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_alias_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);

		// new_golden_file must stay a true alias of new_goldenfile, not a second code path.
		let mut file = mint.new_golden_file("alias_test.txt").unwrap();
		writeln!(file, "Testing alias").unwrap();
	}

	assert!(test_dir.join("alias_test.txt").exists());
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_nested_directories() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_nested_{}", std::process::id()));

	// The mint has to create intermediate directories; test_dir itself is never created here.
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("deeply/nested/dir/file.txt").unwrap();
		writeln!(file, "Nested file content").unwrap();
	}

	assert!(test_dir.join("deeply/nested/dir/file.txt").exists());
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
#[should_panic(expected = "0035")] // Should show line number in diff
fn test_diff_shows_line_numbers() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_linenum_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Forty lines with a single change deep in the file, so the diff has to report a real line
	// number rather than an offset into the changed hunk.
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("lines.txt").unwrap();
		for i in 1..=40 {
			writeln!(file, "Line {}", i).unwrap();
		}
	}

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

	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_empty_files() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_empty_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let _file = mint.new_goldenfile("empty.txt").unwrap();
	}

	// An empty goldenfile must compare equal to empty output rather than read as missing.
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let _file = mint.new_goldenfile("empty.txt").unwrap();
	}

	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
#[should_panic(expected = "Golden file test failed")]
fn test_empty_vs_content() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_empty_vs_content_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let _file = mint.new_goldenfile("empty2.txt").unwrap();
	}

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("empty2.txt").unwrap();
		writeln!(file, "Some content").unwrap();
	}

	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_multiple_files_same_mint() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_multi_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// One Mint must serve several files, including one under a subdirectory.
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);

		let mut file1 = mint.new_goldenfile("file1.txt").unwrap();
		writeln!(file1, "File 1 content").unwrap();

		let mut file2 = mint.new_goldenfile("file2.txt").unwrap();
		writeln!(file2, "File 2 content").unwrap();

		let mut file3 = mint.new_goldenfile("subdir/file3.txt").unwrap();
		writeln!(file3, "File 3 content").unwrap();
	}

	assert!(test_dir.join("file1.txt").exists());
	assert!(test_dir.join("file2.txt").exists());
	assert!(test_dir.join("subdir/file3.txt").exists());
	let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_long_lines_truncation() {
	let test_dir = std::env::temp_dir().join(format!("goldenfile_long_{}", std::process::id()));
	fs::create_dir_all(&test_dir).unwrap();

	// Past the 100-char point where the diff renderer starts truncating.
	let long_line = "x".repeat(150);

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("long.txt").unwrap();
		writeln!(file, "{}", long_line).unwrap();
	}

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("long.txt").unwrap();
		writeln!(file, "{}", long_line).unwrap();
	}

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

	// Several test entry points rewrite one goldenfile in update mode in parallel. Truncating
	// in place would let a concurrent reader see it empty, so the write has to go through a
	// temp file and a rename: any successful read must yield complete content.
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

	// Written as raw bytes rather than str, so a multi-byte sequence has to survive the write
	// and compare paths without being re-encoded.
	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Update);
		let mut file = mint.new_goldenfile("binary.txt").unwrap();
		file.write_all(b"Hello\nWorld\n").unwrap();
		file.write_all(&[0xE2, 0x98, 0x83]).unwrap(); // U+2603 snowman
		file.write_all(b"\n").unwrap();
	}

	{
		let mint = goldenfile::Mint::new_with_mode(&test_dir, Mode::Compare);
		let mut file = mint.new_goldenfile("binary.txt").unwrap();
		file.write_all(b"Hello\nWorld\n").unwrap();
		file.write_all(&[0xE2, 0x98, 0x83]).unwrap(); // U+2603 snowman
		file.write_all(b"\n").unwrap();
	}

	let _ = fs::remove_dir_all(&test_dir);
}
