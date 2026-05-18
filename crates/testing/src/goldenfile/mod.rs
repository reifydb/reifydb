// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	env,
	fs::{self, File, OpenOptions},
	io::{self, Write},
	path::{Path, PathBuf},
	process::id,
	thread, time,
	time::SystemTime,
};

use fs::read;
use reifydb_core::util::colored::Colorize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
	Update,

	Compare,
}

pub struct Mint {
	dir: PathBuf,
	tempdir: Option<PathBuf>,
}

impl Mint {
	pub fn new_with_mode<P: AsRef<Path>>(dir: P, mode: Mode) -> Self {
		let dir = dir.as_ref().to_path_buf();

		match mode {
			Mode::Update => Self {
				dir,
				tempdir: None,
			},
			Mode::Compare => {
				#[allow(clippy::disallowed_methods)]
				let tempdir = env::temp_dir().join(format!(
					"goldenfiles-{}-{}-{:?}",
					id(),
					SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_nanos(),
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

	pub fn new<P: AsRef<Path>>(dir: P) -> Self {
		let dir = dir.as_ref().to_path_buf();

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

	pub fn new_goldenfile<P: AsRef<Path>>(&self, name: P) -> io::Result<GoldenFile> {
		let name = name.as_ref();
		let golden_path = self.dir.join(name);

		if let Some(parent) = golden_path.parent() {
			fs::create_dir_all(parent)?;
		}

		if let Some(ref tempdir) = self.tempdir {
			let temp_path = tempdir.join(name);

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
			let file = OpenOptions::new().write(true).create(true).truncate(true).open(&golden_path)?;

			Ok(GoldenFile {
				file,
				temp_path: None,
				golden_path,
			})
		}
	}

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

pub fn create_diff(expected: &str, actual: &str) -> String {
	let mut output = String::new();

	let expected_lines: Vec<&str> = expected.lines().collect();
	let actual_lines: Vec<&str> = actual.lines().collect();

	let mut differences = Vec::new();
	let max_lines = expected_lines.len().max(actual_lines.len());

	for i in 0..max_lines {
		let expected_line = expected_lines.get(i).copied();
		let actual_line = actual_lines.get(i).copied();

		if expected_line != actual_line {
			differences.push(i);
		}
	}

	if differences.is_empty() {
		output.push_str(&format!("{}\n", "Files are identical but binary comparison failed.".yellow()));
		return output;
	}

	output.clear();

	let context_lines = 3;
	let mut hunks = Vec::new();
	let mut current_hunk: Option<(usize, usize)> = None;

	for &diff_line in &differences {
		match current_hunk {
			None => {
				let start = diff_line.saturating_sub(context_lines);
				current_hunk = Some((start, diff_line + 1));
			}
			Some((start, end)) => {
				if diff_line <= end + context_lines {
					current_hunk = Some((start, diff_line + 1));
				} else {
					hunks.push((start, (end + context_lines).min(max_lines)));
					let new_start = diff_line.saturating_sub(context_lines);
					current_hunk = Some((new_start, diff_line + 1));
				}
			}
		}
	}

	if let Some((start, end)) = current_hunk {
		hunks.push((start, (end + context_lines).min(max_lines)));
	}

	let hunks_to_show = hunks.iter().take(20).cloned().collect::<Vec<_>>();
	let remaining_hunks = hunks.len().saturating_sub(20);

	for (hunk_start, hunk_end) in &hunks_to_show {
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

		for i in *hunk_start..*hunk_end {
			let line_num = i + 1;
			let expected_line = expected_lines.get(i).copied();
			let actual_line = actual_lines.get(i).copied();

			match (expected_line, actual_line) {
				(Some(e), Some(a)) if e == a => {
					output.push_str(&format!(
						"{}  {}\n",
						format!("{:04}", line_num).bright_black(),
						e
					));
				}
				(Some(e), Some(a)) => {
					output.push_str(&format!(
						"{} {}{}\n",
						format!("{:04}", line_num).bright_black(),
						"-".red(),
						e.red()
					));
					output.push_str(&format!("     {}{}\n", "+".green(), a.green()));
				}
				(Some(e), None) => {
					output.push_str(&format!(
						"{} {}{}\n",
						format!("{:04}", line_num).bright_black(),
						"-".red(),
						e.red()
					));
				}
				(None, Some(a)) => {
					output.push_str(&format!(
						"{} {}{}\n",
						format!("{:04}", line_num).bright_black(),
						"+".green(),
						a.green()
					));
				}
				(None, None) => unreachable!(),
			}
		}
	}

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
