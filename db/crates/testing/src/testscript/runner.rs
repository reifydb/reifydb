// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{env::temp_dir, error::Error, io::Write as _};

use crate::testscript::{Command, parser::parse};

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
pub fn run_path<R: Runner, P: AsRef<std::path::Path>>(runner: &mut R, path: P) -> std::io::Result<()> {
	let path = path.as_ref();
	let Some(dir) = path.parent() else {
		return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid path '{path:?}'")));
	};
	let Some(filename) = path.file_name() else {
		return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid path '{path:?}'")));
	};

	if filename.to_str().unwrap().ends_with(".skip") {
		return Ok(());
	}

	let input = std::fs::read_to_string(dir.join(filename))?;
	let output = generate(runner, &input)?;

	crate::goldenfile::Mint::new(dir).new_goldenfile(filename)?.write_all(output.as_bytes())
}

pub fn run<R: Runner, S: Into<String>>(runner: R, test: S) {
	try_run(runner, test).unwrap();
}

pub fn try_run<R: Runner, S: Into<String>>(mut runner: R, test: S) -> std::io::Result<()> {
	let input = test.into();

	let dir = temp_dir();
	let file_name = format!(
		"test-{}-{}.txt",
		std::process::id(),
		std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
	);
	let file_path = dir.join(&file_name);

	let mut file = std::fs::File::create(&file_path)?;
	file.write_all(input.as_bytes())?;

	let output = generate(&mut runner, &input)?;
	crate::goldenfile::Mint::new(dir).new_goldenfile(&file_name)?.write_all(output.as_bytes())
}

/// Generates output for a testscript input, without comparing them.
pub fn generate<R: Runner>(runner: &mut R, input: &str) -> std::io::Result<String> {
	let mut output = String::with_capacity(input.len()); // common case: output == input

	// Detect end-of-line format.
	let eol = match input.find("\r\n") {
		Some(_) => "\r\n",
		None => "\n",
	};

	// Parse the script.
	let blocks = parse(input).map_err(|e| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			format!(
				"parse error at line {} column {} for {:?}:\n{}\n{}^",
				e.input.location_line(),
				e.input.get_column(),
				e.code,
				String::from_utf8_lossy(e.input.get_line_beginning()),
				' '.to_string().repeat(e.input.get_utf8_column() - 1)
			),
		)
	})?;

	// Call the start_script() hook.
	runner.start_script()
		.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("start_script failed: {e}")))?;

	for (i, block) in blocks.iter().enumerate() {
		// There may be a trailing block with no commands if the script
		// has bare comments at the end. If so, just retain its
		// literal contents.
		if block.commands.is_empty() {
			output.push_str(&block.literal);
			continue;
		}

		// Process each block of commands and accumulate their output.
		let mut block_output = String::new();

		// Call the start_block() hook.
		block_output.push_str(&ensure_eol(
			runner.start_block().map_err(|e| {
				std::io::Error::new(
					std::io::ErrorKind::Other,
					format!("start_block failed at line {}: {e}", block.line_number),
				)
			})?,
			eol,
		));

		for command in &block.commands {
			let mut command_output = String::new();

			// Call the start_command() hook.
			command_output.push_str(&ensure_eol(
				runner.start_command(command).map_err(|e| {
					std::io::Error::new(
						std::io::ErrorKind::Other,
						format!("start_command failed at line {}: {e}", command.line_number),
					)
				})?,
				eol,
			));

			// Execute the command. Handle panics and errors if
			// requested. We assume the command is unwind-safe
			// when handling panics, it is up to callers to
			// manage this appropriately.
			let run = std::panic::AssertUnwindSafe(|| runner.run(command));
			command_output.push_str(&match std::panic::catch_unwind(run) {
				// Unexpected success, error out.
				Ok(Ok(output)) if command.fail => {
					return Err(std::io::Error::new(
						std::io::ErrorKind::Other,
						format!(
							"expected command '{}' to fail at line {}, succeeded with: {output}",
							command.name, command.line_number
						),
					));
				}

				// Expected success, output the result.
				Ok(Ok(output)) => output,

				// Expected error, output it.
				Ok(Err(e)) if command.fail => {
					format!("{e}")
				}

				// Unexpected error, return it.
				Ok(Err(e)) => {
					return Err(std::io::Error::new(
						std::io::ErrorKind::Other,
						format!(
							"command '{}' failed at line {}: {e}",
							command.name, command.line_number
						),
					));
				}

				// Expected panic, output it.
				Err(panic) if command.fail => {
					let message = panic
						.downcast_ref::<&str>()
						.map(|s| s.to_string())
						.or_else(|| panic.downcast_ref::<String>().cloned())
						.unwrap_or_else(|| std::panic::resume_unwind(panic));
					format!("Panic: {message}")
				}

				// Unexpected panic, throw it.
				Err(panic) => std::panic::resume_unwind(panic),
			});

			// Make sure the command output has a trailing newline,
			// unless empty.
			command_output = ensure_eol(command_output, eol);

			// Call the end_command() hook.
			command_output.push_str(&ensure_eol(
				runner.end_command(command).map_err(|e| {
					std::io::Error::new(
						std::io::ErrorKind::Other,
						format!("end_command failed at line {}: {e}", command.line_number),
					)
				})?,
				eol,
			));

			// Silence the output if requested.
			if command.silent {
				command_output = "".to_string();
			}

			// Prefix output lines if requested.
			if let Some(prefix) = &command.prefix {
				if !command_output.is_empty() {
					command_output = format!(
						"{prefix}: {}{eol}",
						command_output
							.strip_suffix(eol)
							.unwrap_or(command_output.as_str())
							.replace('\n', &format!("\n{prefix}: "))
					);
				}
			}

			block_output.push_str(&command_output);
		}

		// Call the end_block() hook.
		block_output.push_str(&ensure_eol(
			runner.end_block().map_err(|e| {
				std::io::Error::new(
					std::io::ErrorKind::Other,
					format!("end_block failed at line {}: {e}", block.line_number),
				)
			})?,
			eol,
		));

		// If the block doesn't have any output, default to "ok".
		if block_output.is_empty() {
			block_output.push_str("ok\n")
		}

		// If the block output contains blank lines, use a > prefix for
		// it.
		//
		// We'd be better off using regular expressions here, but don't
		// want to add a dependency just for this.
		if block_output.starts_with('\n')
			|| block_output.starts_with("\r\n")
			|| block_output.contains("\n\n")
			|| block_output.contains("\n\r\n")
		{
			block_output = format!("> {}", block_output.replace('\n', "\n> "));
			// We guarantee above that block output ends with a
			// newline, so we remove the "> " at the end of the
			// output.
			block_output.pop();
			block_output.pop();
		}

		// Add the resulting block to the output. If this is not the
		// last block, also add a newline separator.
		output.push_str(&format!("{}---{eol}{}", block.literal, block_output));
		if i < blocks.len() - 1 {
			output.push_str(eol);
		}
	}

	// Call the end_script() hook.
	runner.end_script()
		.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("end_script failed: {e}")))?;

	Ok(output)
}

/// Appends a newline if the string is not empty and doesn't already have one.
fn ensure_eol(mut s: String, eol: &str) -> String {
	if let Some(c) = s.chars().next_back() {
		if c != '\n' {
			s.push_str(eol)
		}
	}
	s
}

// NB: most tests are done as testscripts under tests/.
#[cfg(test)]
mod tests {
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
