// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{env::temp_dir, error::Error, fs, io, io::Write as _, panic, path, process, time};

use fs::read_to_string;
use io::ErrorKind;
use panic::AssertUnwindSafe;
use path::Path;
use time::SystemTime;

use crate::{
	goldenfile::Mint,
	testscript::{
		command::{Block, Command},
		parser::parse,
	},
};

/// Runs testscript commands, returning their output.
pub trait Runner {
	/// Runs a testscript command, returning its output, or an error if the
	/// command fails.
	///
	/// Arguments can be accessed directly via [`Command::args`], or by
	/// using the [`Command::consume_args`] helper for more convenient
	/// processing.
	///
	/// Error cases are typically tested by running the command with a `!`
	/// prefix (expecting a failure), but the runner can also handle these
	/// itself and return an `Ok` result with appropriate output.
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>>;

	/// Called at the start of a testscript. Used e.g. for initial setup.
	/// Can't return output, since it's not called in the context of a
	/// block.
	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		Ok(())
	}

	/// Called at the end of a testscript. Used e.g. for state assertions.
	/// Can't return output, since it's not called in the context of a
	/// block.
	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		Ok(())
	}

	/// Called at the start of a block. Used e.g. to output initial state.
	/// Any output is prepended to the block's output.
	fn start_block(&mut self) -> Result<String, Box<dyn Error>> {
		Ok(String::new())
	}

	/// Called at the end of a block. Used e.g. to output final state.
	/// Any output is appended to the block's output.
	fn end_block(&mut self) -> Result<String, Box<dyn Error>> {
		Ok(String::new())
	}

	/// Called at the start of a command. Used e.g. for setup. Any output is
	/// prepended to the command's output, and is affected e.g. by the
	/// prefix and silencing of the command.
	#[allow(unused_variables)]
	fn start_command(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		Ok(String::new())
	}

	/// Called at the end of a command. Used e.g. for cleanup. Any output is
	/// appended to the command's output, and is affected e.g. by the prefix
	/// and silencing of the command.
	#[allow(unused_variables)]
	fn end_command(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		Ok(String::new())
	}
}

/// Runs a testscript at the given path.
///
/// Panics if the script output differs from the current input file. Errors on
/// IO, parser, or runner failure. If the environment variable
/// `UPDATE_TESTFILES=1` is set, the new output file will replace the input
/// file.
pub fn run_path<R: Runner, P: AsRef<Path>>(runner: &mut R, path: P) -> io::Result<()> {
	let path = path.as_ref();
	let Some(dir) = path.parent() else {
		return Err(io::Error::new(ErrorKind::InvalidInput, format!("invalid path '{path:?}'")));
	};
	let Some(filename) = path.file_name() else {
		return Err(io::Error::new(ErrorKind::InvalidInput, format!("invalid path '{path:?}'")));
	};

	if filename.to_str().unwrap().ends_with(".skip") {
		return Ok(());
	}

	let input = read_to_string(dir.join(filename))?;
	let output = generate(runner, &input)?;

	Mint::new(dir).new_goldenfile(filename)?.write_all(output.as_bytes())
}

pub fn run<R: Runner, S: Into<String>>(runner: R, test: S) {
	try_run(runner, test).unwrap();
}

pub fn try_run<R: Runner, S: Into<String>>(mut runner: R, test: S) -> io::Result<()> {
	let input = test.into();

	let dir = temp_dir();
	#[allow(clippy::disallowed_methods)]
	let file_name = format!(
		"test-{}-{}.txt",
		process::id(),
		SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_nanos()
	);
	let file_path = dir.join(&file_name);

	let mut file = fs::File::create(&file_path)?;
	file.write_all(input.as_bytes())?;

	let output = generate(&mut runner, &input)?;
	Mint::new(dir).new_goldenfile(&file_name)?.write_all(output.as_bytes())
}

/// Generates output for a testscript input, without comparing them.
pub fn generate<R: Runner>(runner: &mut R, input: &str) -> io::Result<String> {
	let mut output = String::with_capacity(input.len()); // common case: output == input
	let eol = detect_eol(input);
	let blocks = parse_blocks(input)?;

	runner.start_script().map_err(|e| io::Error::other(format!("start_script failed: {e}")))?;

	for (i, block) in blocks.iter().enumerate() {
		if block.commands.is_empty() {
			output.push_str(&block.literal);
			continue;
		}
		let block_output = process_block(runner, block, eol)?;
		output.push_str(&format!("{}---{eol}{}", block.literal, block_output));
		if i < blocks.len() - 1 {
			output.push_str(eol);
		}
	}

	runner.end_script().map_err(|e| io::Error::other(format!("end_script failed: {e}")))?;
	Ok(output)
}

#[inline]
fn detect_eol(input: &str) -> &'static str {
	if input.contains("\r\n") {
		"\r\n"
	} else {
		"\n"
	}
}

#[inline]
fn parse_blocks(input: &str) -> io::Result<Vec<Block>> {
	parse(input).map_err(|e| {
		io::Error::new(
			ErrorKind::InvalidInput,
			format!(
				"parse error at line {} column {} for {:?}:\n{}\n{}^",
				e.input.location_line(),
				e.input.get_column(),
				e.code,
				String::from_utf8_lossy(e.input.get_line_beginning()),
				' '.to_string().repeat(e.input.get_utf8_column() - 1)
			),
		)
	})
}

fn process_block<R: Runner>(runner: &mut R, block: &Block, eol: &str) -> io::Result<String> {
	let mut block_output = String::new();
	block_output.push_str(&ensure_eol(
		runner.start_block().map_err(|e| {
			io::Error::other(format!("start_block failed at line {}: {e}", block.line_number))
		})?,
		eol,
	));
	for command in &block.commands {
		let command_output = process_command(runner, command, eol)?;
		block_output.push_str(&command_output);
	}
	block_output.push_str(&ensure_eol(
		runner.end_block().map_err(|e| {
			io::Error::other(format!("end_block failed at line {}: {e}", block.line_number))
		})?,
		eol,
	));
	if block_output.is_empty() {
		block_output.push_str("ok\n");
	}
	Ok(apply_blank_line_prefix(block_output))
}

fn process_command<R: Runner>(runner: &mut R, command: &Command, eol: &str) -> io::Result<String> {
	let mut command_output = String::new();
	command_output.push_str(&ensure_eol(
		runner.start_command(command).map_err(|e| {
			io::Error::other(format!("start_command failed at line {}: {e}", command.line_number))
		})?,
		eol,
	));
	command_output.push_str(&run_command_with_panic_handling(runner, command)?);
	command_output = ensure_eol(command_output, eol);
	command_output.push_str(&ensure_eol(
		runner.end_command(command).map_err(|e| {
			io::Error::other(format!("end_command failed at line {}: {e}", command.line_number))
		})?,
		eol,
	));
	if command.silent {
		command_output.clear();
	}
	if let Some(prefix) = &command.prefix
		&& !command_output.is_empty()
	{
		command_output = format!(
			"{prefix}: {}{eol}",
			command_output
				.strip_suffix(eol)
				.unwrap_or(command_output.as_str())
				.replace('\n', &format!("\n{prefix}: "))
		);
	}
	Ok(command_output)
}

fn run_command_with_panic_handling<R: Runner>(runner: &mut R, command: &Command) -> io::Result<String> {
	let run = AssertUnwindSafe(|| runner.run(command));
	match panic::catch_unwind(run) {
		Ok(Ok(output)) if command.fail => Err(io::Error::other(format!(
			"expected command '{}' to fail at line {}, succeeded with: {output}",
			command.name, command.line_number
		))),
		Ok(Ok(output)) => Ok(output),
		Ok(Err(e)) if command.fail => Ok(format!("{e}")),
		Ok(Err(e)) => Err(io::Error::other(format!(
			"command '{}' failed at line {}: {e}",
			command.name, command.line_number
		))),
		Err(panic) if command.fail => {
			let message = panic
				.downcast_ref::<&str>()
				.map(|s| s.to_string())
				.or_else(|| panic.downcast_ref::<String>().cloned())
				.unwrap_or_else(|| panic::resume_unwind(panic));
			Ok(format!("Panic: {message}"))
		}
		Err(panic) => panic::resume_unwind(panic),
	}
}

#[inline]
fn apply_blank_line_prefix(mut block_output: String) -> String {
	if block_output.starts_with('\n')
		|| block_output.starts_with("\r\n")
		|| block_output.contains("\n\n")
		|| block_output.contains("\n\r\n")
	{
		block_output = format!("> {}", block_output.replace('\n', "\n> "));
		block_output = block_output.replace("> \n", ">\n");
		block_output.pop();
		block_output.pop();
	}
	block_output
}

/// Appends a newline if the string is not empty and doesn't already have one.
fn ensure_eol(mut s: String, eol: &str) -> String {
	if let Some(c) = s.chars().next_back()
		&& c != '\n'
	{
		s.push_str(eol)
	}
	s
}

// NB: most tests are done as testscripts under tests/.
#[cfg(test)]
pub mod tests {
	use super::*;

	/// A runner which simply counts the number of times its hooks are
	/// called.
	#[derive(Default)]
	struct HookRunner {
		start_script_count: usize,
		end_script_count: usize,
		start_block_count: usize,
		end_block_count: usize,
		start_command_count: usize,
		end_command_count: usize,
	}

	impl Runner for HookRunner {
		fn run(&mut self, _: &Command) -> Result<String, Box<dyn Error>> {
			Ok(String::new())
		}

		fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
			self.start_script_count += 1;
			Ok(())
		}

		fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
			self.end_script_count += 1;
			Ok(())
		}

		fn start_block(&mut self) -> Result<String, Box<dyn Error>> {
			self.start_block_count += 1;
			Ok(String::new())
		}

		fn end_block(&mut self) -> Result<String, Box<dyn Error>> {
			self.end_block_count += 1;
			Ok(String::new())
		}

		fn start_command(&mut self, _: &Command) -> Result<String, Box<dyn Error>> {
			self.start_command_count += 1;
			Ok(String::new())
		}

		fn end_command(&mut self, _: &Command) -> Result<String, Box<dyn Error>> {
			self.end_command_count += 1;
			Ok(String::new())
		}
	}

	/// Tests that runner hooks are called as expected.
	#[test]
	fn hooks() {
		let mut runner = HookRunner::default();
		generate(
			&mut runner,
			r#"
command
---

command
command
---
"#,
		)
		.unwrap();

		assert_eq!(runner.start_script_count, 1);
		assert_eq!(runner.end_script_count, 1);
		assert_eq!(runner.start_block_count, 2);
		assert_eq!(runner.end_block_count, 2);
		assert_eq!(runner.start_command_count, 3);
		assert_eq!(runner.end_command_count, 3);
	}
}
