// This file includes portions of code from https://github.com/erikgrinaker/goldenscript (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

#![warn(clippy::all)]

use std::{error::Error, io::Write as _};

use reifydb_testing::testscript;
use test_each_file::test_each_path;

// Run testscripts in tests/scripts that debug-print the commands.
test_each_path! { in "crates/reifydb-testing/tests/testscript/scripts" as scripts => test_testscript }

fn test_testscript(path: &std::path::Path) {
	testscript::run_path(&mut DebugRunner::new(), path)
		.expect("runner failed")
}

// Run testscripts in tests/generate with output in a separate file. This is
// particularly useful for parser tests where output hasn't yet been generated.
test_each_path! { for ["in", "out"] in "crates/reifydb-testing/tests/testscript/generate" as generate => test_generate }

fn test_generate([in_path, out_path]: [&std::path::Path; 2]) {
	let input =
		std::fs::read_to_string(in_path).expect("failed to read file");
	let output = testscript::generate(&mut DebugRunner::new(), &input)
		.expect("runner failed");

	let dir = out_path.parent().expect("invalid path");
	let filename = out_path.file_name().expect("invalid path");
	let mint = reifydb_testing::goldenfile::Mint::new(dir);
	let mut f = mint
		.new_goldenfile(filename)
		.expect("failed to create goldenfile");
	f.write_all(output.as_bytes()).expect("failed to write output");
}

// Generate error tests for each pair of *.in and *.error files in
// tests/errors/. The input scripts are expected to error or panic with the
// stored output.
test_each_path! { for ["in", "error"] in "crates/reifydb-testing/tests/testscript/errors" as errors => test_error }

fn test_error([in_path, out_path]: [&std::path::Path; 2]) {
	let input =
		std::fs::read_to_string(in_path).expect("failed to read file");
	let run = std::panic::AssertUnwindSafe(|| {
		testscript::generate(&mut DebugRunner::new(), &input)
	});
	let message = match std::panic::catch_unwind(run) {
		Ok(Ok(_)) => panic!("script succeeded"),
		Ok(Err(e)) => e.to_string(),
		Err(panic) => panic
			.downcast_ref::<&str>()
			.map(|s| s.to_string())
			.or_else(|| panic.downcast_ref::<String>().cloned())
			.unwrap_or_else(|| std::panic::resume_unwind(panic)),
	};

	let dir = out_path.parent().expect("invalid path");
	let filename = out_path.file_name().expect("invalid path");
	let mint = reifydb_testing::goldenfile::Mint::new(dir);
	let mut f = mint
		.new_goldenfile(filename)
		.expect("failed to create goldenfile");
	f.write_all(message.as_bytes()).expect("failed to write goldenfile");
}

/// A testscript runner that debug-prints the parsed command. It
/// understands the following special commands:
///
/// _echo: prints back the arguments, space-separated
/// _error: errors with the given string
/// _panic: panics with the given string
/// _set: sets various options
///
///   - prefix=<string>: printed immediately before the command output
///   - suffix=<string>: printed immediately after the command output
///   - start_block=<string>: printed at the start of a block
///   - start_command=<string>: printed at the start of a command
///   - end_block=<string>: printed at the end of a block
///   - end_command=<string>: printed at the end of a command
///
/// If a command is expected to fail via !, the parsed command string is
/// returned as an error.
#[derive(Default)]
struct DebugRunner {
	prefix: String,
	suffix: String,
	start_block: String,
	end_block: String,
	start_command: String,
	end_command: String,
}

impl DebugRunner {
	fn new() -> Self {
		Self::default()
	}
}

impl testscript::Runner for DebugRunner {
	fn run(
		&mut self,
		command: &testscript::Command,
	) -> Result<String, Box<dyn Error>> {
		// Process commands.
		let output = match command.name.as_str() {
			"_echo" => {
				for arg in &command.args {
					if arg.key.is_some() {
						return Err("echo args can't have keys".into());
					}
				}
				command.args
					.iter()
					.map(|a| a.value.clone())
					.collect::<Vec<String>>()
					.join(" ")
			}

			"_error" => {
				let message = command
					.args
					.first()
					.map(|a| a.value.as_str())
					.unwrap_or("error");
				return Err(message.to_string().into());
			}

			"_panic" => {
				let message = command
					.args
					.first()
					.map(|a| a.value.as_str())
					.unwrap_or("panic");
				panic!("{message}");
			}

			"_set" => {
				for arg in &command.args {
					match arg.key.as_deref() {
                        Some("prefix") => self.prefix = arg.value.clone(),
                        Some("suffix") => self.suffix = arg.value.clone(),
                        Some("start_block") => self.start_block = arg.value.clone(),
                        Some("end_block") => self.end_block = arg.value.clone(),
                        Some("start_command") => self.start_command = arg.value.clone(),
                        Some("end_command") => self.end_command = arg.value.clone(),
                        Some(key) => return Err(format!("unknown argument key {key}").into()),
                        None => return Err("argument must have a key".into())}
				}
				return Ok(String::new());
			}

			_ if command.fail => {
				return Err(format!("{command:?}").into());
			}

			_ => format!("{command:?}"),
		};

		Ok(format!("{}{output}{}", self.prefix, self.suffix))
	}

	fn start_block(&mut self) -> Result<String, Box<dyn Error>> {
		Ok(self.start_block.clone())
	}

	fn end_block(&mut self) -> Result<String, Box<dyn Error>> {
		Ok(self.end_block.clone())
	}

	fn start_command(
		&mut self,
		_: &testscript::Command,
	) -> Result<String, Box<dyn Error>> {
		Ok(self.start_command.clone())
	}

	fn end_command(
		&mut self,
		_: &testscript::Command,
	) -> Result<String, Box<dyn Error>> {
		Ok(self.end_command.clone())
	}
}
