// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	env,
	fs::{self, File, OpenOptions},
	io::{self, Write},
	path::{Path, PathBuf},
	process::id,
	thread,
	time::SystemTime,
};

use fs::read;
use reifydb_core::util::colored::Colorize;

/// Test mode for goldenfile operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
	/// Update mode: write directly to golden files
	Update,
	/// Compare mode: write to temp files and compare with golden files
	Compare,
}

/// Manages goldenfile creation and comparison for testing
pub struct Mint {
	dir: PathBuf,
	tempdir: Option<PathBuf>,
}

impl Mint {
	/// Creates a new Mint instance for the given directory with explicit
	/// mode
	pub fn new_with_mode<P: AsRef<Path>>(dir: P, mode: Mode) -> Self {
		let dir = dir.as_ref().to_path_buf();

		match mode {
			Mode::Update => Self {
				dir,
				tempdir: None,
			},
			Mode::Compare => {
				// In test mode, write to a temp directory first
				// Use a more unique temp directory name to
				// avoid conflicts Include thread ID for
				// better uniqueness in concurrent scenarios
				let tempdir = env::temp_dir().join(format!(
					"goldenfiles-{}-{}-{:?}",
					id(),
					SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
					thread::current().id()
				));
				fs::create_dir_all(&tempdir).ok();

				Self {
					dir,
					tempdir: Some(tempdir),
				}
			}
		}
	}

	/// Creates a new Mint instance for the given directory
	/// This method preserves backward compatibility by checking environment
	/// variables
	pub fn new<P: AsRef<Path>>(dir: P) -> Self {
		let dir = dir.as_ref().to_path_buf();

		// Check if we should update goldenfiles
		let should_update = env::var("UPDATE_TESTFILE").is_ok()
			|| env::var("UPDATE_TESTFILES").is_ok()
			|| env::var("UPDATE_GOLDENFILE").is_ok()
			|| env::var("UPDATE_GOLDENFILES").is_ok();

		let mode = if should_update {
			Mode::Update
		} else {
			Mode::Compare
		};

		Self::new_with_mode(dir, mode)
	}

	/// Creates a new golden file with the given name
	pub fn new_goldenfile<P: AsRef<Path>>(&self, name: P) -> io::Result<GoldenFile> {
		let name = name.as_ref();
		let golden_path = self.dir.join(name);

		// Ensure parent directory exists
		if let Some(parent) = golden_path.parent() {
			fs::create_dir_all(parent)?;
		}

		if let Some(ref tempdir) = self.tempdir {
			// Test mode: write to temp file and compare later
			let temp_path = tempdir.join(name);

			// Ensure temp parent directory exists
			if let Some(parent) = temp_path.parent() {
				fs::create_dir_all(parent)?;
			}

			let file = OpenOptions::new().write(true).create(true).truncate(true).open(&temp_path)?;

			Ok(GoldenFile {
				file,
				temp_path: Some(temp_path),
				golden_path,
			})
		} else {
			// Update mode: write directly to golden file
			let file = OpenOptions::new().write(true).create(true).truncate(true).open(&golden_path)?;

			Ok(GoldenFile {
				file,
				temp_path: None,
				golden_path,
			})
		}
	}

	/// Alias for new_goldenfile for compatibility
	pub fn new_golden_file<P: AsRef<Path>>(&self, name: P) -> io::Result<GoldenFile> {
		self.new_goldenfile(name)
	}
}

impl Drop for Mint {
	fn drop(&mut self) {
		if let Some(ref dir) = self.tempdir {
			let _ = fs::remove_dir_all(dir);
		}
	}
}

pub struct GoldenFile {
	file: File,
	temp_path: Option<PathBuf>,
	golden_path: PathBuf,
}

impl Write for GoldenFile {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.file.write(buf)
	}

	fn flush(&mut self) -> io::Result<()> {
		self.file.flush()
	}
}

impl Drop for GoldenFile {
	fn drop(&mut self) {
		let _ = self.file.flush();

		// If we have a temp path, compare with golden file
		if let Some(ref temp_path) = self.temp_path {
			if !self.golden_path.exists() {
				panic!(
					"{}\n{}\n\n{}",
					format!("Golden file '{}' does not exist", self.golden_path.display())
						.red()
						.bold(),
					"Run with UPDATE_TESTFILES=1 to create it.".yellow(),
					format!("Would create: {}", self.golden_path.display()).bright_black()
				);
			}

			let temp_content = read(temp_path).unwrap_or_default();
			let golden_content = read(&self.golden_path).unwrap_or_default();

			if temp_content != golden_content {
				let temp_str = String::from_utf8_lossy(&temp_content);
				let golden_str = String::from_utf8_lossy(&golden_content);

				// Create a git-like diff
				let diff_output = create_diff(&golden_str, &temp_str);

				panic!(
					"{}\n\n{}\n\n{}",
					format!("Golden file test failed for '{}'", self.golden_path.display())
						.red()
						.bold(),
					diff_output,
					"Run with UPDATE_TESTFILES=1 to update the goldenfile.".yellow()
				);
			}
		}
	}
}

/// Creates a git-like unified diff between expected and actual content
fn create_diff(expected: &str, actual: &str) -> String {
	let mut output = String::new();

	// Split into lines for comparison
	let expected_lines: Vec<&str> = expected.lines().collect();
	let actual_lines: Vec<&str> = actual.lines().collect();

	// Find all differences
	let mut differences = Vec::new();
	let max_lines = expected_lines.len().max(actual_lines.len());

	for i in 0..max_lines {
		let expected_line = expected_lines.get(i).copied();
		let actual_line = actual_lines.get(i).copied();

		if expected_line != actual_line {
			differences.push(i);
		}
	}

	// If no differences found, return empty
	if differences.is_empty() {
		output.push_str(&format!("{}\n", "Files are identical but binary comparison failed.".yellow()));
		return output;
	}

	// Clear output for clean diff
	output.clear();

	// Group differences into hunks with context
	let context_lines = 3;
	let mut hunks = Vec::new();
	let mut current_hunk: Option<(usize, usize)> = None;

	for &diff_line in &differences {
		match current_hunk {
			None => {
				// Start a new hunk
				let start = diff_line.saturating_sub(context_lines);
				current_hunk = Some((start, diff_line + 1));
			}
			Some((start, end)) => {
				// Check if this difference is close enough to
				// extend the current hunk
				if diff_line <= end + context_lines {
					// Extend current hunk
					current_hunk = Some((start, diff_line + 1));
				} else {
					// Finish current hunk and start a new
					// one
					hunks.push((start, (end + context_lines).min(max_lines)));
					let new_start = diff_line.saturating_sub(context_lines);
					current_hunk = Some((new_start, diff_line + 1));
				}
			}
		}
	}

	// Add the last hunk
	if let Some((start, end)) = current_hunk {
		hunks.push((start, (end + context_lines).min(max_lines)));
	}

	// Limit to first 3 hunks to reduce noise
	let hunks_to_show = hunks.iter().take(20).cloned().collect::<Vec<_>>();
	let remaining_hunks = hunks.len().saturating_sub(20);

	// Render hunks
	for (hunk_start, hunk_end) in &hunks_to_show {
		// Calculate line numbers for the hunk header
		let expected_start = hunk_start + 1;
		let expected_count = expected_lines[*hunk_start..(*hunk_end).min(expected_lines.len())].len();
		let actual_start = hunk_start + 1;
		let actual_count = actual_lines[*hunk_start..(*hunk_end).min(actual_lines.len())].len();

		output.push_str(&format!(
			"{} -{},{} +{},{} {}\n",
			"@@".bright_cyan(),
			expected_start,
			expected_count,
			actual_start,
			actual_count,
			"@@".bright_cyan()
		));

		// Render hunk content with line numbers
		for i in *hunk_start..*hunk_end {
			let line_num = i + 1; // 1-indexed line number
			let expected_line = expected_lines.get(i).copied();
			let actual_line = actual_lines.get(i).copied();

			// Truncate long lines for readability
			let truncate_line = |line: &str| -> String {
				if line.len() > 100 {
					// Use char boundary-safe truncation
					let mut char_boundary = 97;
					while !line.is_char_boundary(char_boundary) && char_boundary > 0 {
						char_boundary -= 1;
					}
					format!("{}...", &line[..char_boundary])
				} else {
					line.to_string()
				}
			};

			match (expected_line, actual_line) {
				(Some(e), Some(a)) if e == a => {
					// Context line - show line number in
					// gray with 4 digits
					output.push_str(&format!(
						"{}  {}\n",
						format!("{:04}", line_num).bright_black(),
						truncate_line(e)
					));
				}
				(Some(e), Some(a)) => {
					// Changed line - show line number for
					// both
					output.push_str(&format!(
						"{} {}{}\n",
						format!("{:04}", line_num).bright_black(),
						"-".red(),
						truncate_line(e).red()
					));
					output.push_str(&format!("     {}{}\n", "+".green(), truncate_line(a).green()));
				}
				(Some(e), None) => {
					// Deleted line
					output.push_str(&format!(
						"{} {}{}\n",
						format!("{:04}", line_num).bright_black(),
						"-".red(),
						truncate_line(e).red()
					));
				}
				(None, Some(a)) => {
					// Added line
					output.push_str(&format!(
						"{} {}{}\n",
						format!("{:04}", line_num).bright_black(),
						"+".green(),
						truncate_line(a).green()
					));
				}
				(None, None) => unreachable!(),
			}
		}
	}

	// If there are more hunks, indicate that
	if remaining_hunks > 0 {
		output.push_str(&format!(
			"\n{}\n",
			format!(
				"... and {} more difference{}",
				remaining_hunks,
				if remaining_hunks == 1 {
					""
				} else {
					"s"
				}
			)
			.bright_black()
		));
	}

	// Add summary
	let total_diffs = differences.len();
	if total_diffs > 10 {
		output.push_str(&format!(
			"\n{}\n",
			format!(
				"Total: {} line{} differ",
				total_diffs,
				if total_diffs == 1 {
					""
				} else {
					"s"
				}
			)
			.bright_black()
		));
	}

	output
}
