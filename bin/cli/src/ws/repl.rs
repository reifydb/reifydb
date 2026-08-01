// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::io::{self, Write};

use reifydb_client::{Frame, WireFormat, WsClient};
use rustyline::{DefaultEditor, error::ReadlineError};
use terminal_size::{Width, terminal_size};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
enum DisplayMode {
	Truncate,
	Full,
}

enum DotCommandResult {
	Exit,
	Reauthenticate(String),
	Continue,
}

pub async fn start_repl(host: &str, port: u16, token: Option<String>) -> Result<()> {
	let mut client = WsClient::connect(&format!("ws://{}:{}", host, port), WireFormat::Frames)
		.await
		.map_err(|e| format!("Failed to connect to WebSocket server: {}", e))?;

	if let Some(ref token) = token {
		client.authenticate(token).await.map_err(|e| format!("Failed to authenticate: {}", e))?;
	}

	let mut current_token = token;
	let mut display_mode = DisplayMode::Truncate;

	println!("Connected to ws://{}:{}", host, port);
	println!("ValueType .help for help, .quit to exit\n");

	let mut rl = DefaultEditor::new().map_err(|e| format!("Failed to initialize readline: {}", e))?;

	let mut buffer = String::new();

	loop {
		let prompt = if buffer.is_empty() {
			"reifydb> "
		} else {
			"     ... "
		};

		match rl.readline(prompt) {
			Ok(line) => {
				let _ = rl.add_history_entry(&line);

				if buffer.is_empty() && line.trim().starts_with('.') {
					match handle_dot_command(line.trim(), &mut current_token, &mut display_mode) {
						DotCommandResult::Exit => break,
						DotCommandResult::Reauthenticate(new_token) => {
							match client.authenticate(&new_token).await {
								Ok(()) => {
									current_token = Some(new_token);
									println!("Authentication token updated");
								}
								Err(e) => {
									eprintln!(
										"Failed to authenticate with new token: {}",
										e
									);
								}
							}
						}
						DotCommandResult::Continue => {}
					}
					continue;
				}

				buffer.push_str(&line);
				buffer.push(' ');

				if line.trim().ends_with(';') {
					let statement = buffer.trim().to_string();
					buffer.clear();

					if !statement.is_empty() {
						execute_query(&mut client, &statement, display_mode).await;
					}
				}
			}
			Err(ReadlineError::Interrupted) => {
				println!("^C");
				buffer.clear();
			}
			Err(ReadlineError::Eof) => {
				println!("Goodbye!");
				break;
			}
			Err(err) => {
				eprintln!("Error: {}", err);
				break;
			}
		}
	}

	let _ = client.close().await;

	Ok(())
}

fn handle_dot_command(
	cmd: &str,
	current_token: &mut Option<String>,
	display_mode: &mut DisplayMode,
) -> DotCommandResult {
	let parts: Vec<&str> = cmd.split_whitespace().collect();
	let command = parts[0];

	match command {
		".quit" | ".exit" => {
			println!("Goodbye!");
			DotCommandResult::Exit
		}
		".help" => {
			println!("Available commands:");
			println!("  .quit, .exit      - Exit the REPL");
			println!("  .clear            - Clear the screen");
			println!("  .token [TOKEN]    - Set or show authentication token");
			println!("  .mode [truncate|full] - Set display mode");
			println!("  .help             - Show this help message");
			DotCommandResult::Continue
		}
		".clear" => {
			// \x1B[2J - Clear entire screen
			// \x1B[3J - Clear scrollback buffer
			// \x1B[H - Move cursor to home position (1,1)
			print!("\x1B[2J\x1B[3J\x1B[H");
			io::stdout().flush().unwrap();
			DotCommandResult::Continue
		}
		".mode" => {
			if parts.len() == 1 {
				let mode_str = match display_mode {
					DisplayMode::Truncate => "truncate",
					DisplayMode::Full => "full",
				};
				println!("Current display mode: {}", mode_str);
			} else {
				match parts[1] {
					"truncate" => {
						*display_mode = DisplayMode::Truncate;
						println!("Display mode set to: truncate (auto-fit terminal width)");
					}
					"full" => {
						*display_mode = DisplayMode::Full;
						println!("Display mode set to: full (allow overflow)");
					}
					_ => {
						println!("Unknown mode: {}. Use 'truncate' or 'full'", parts[1]);
					}
				}
			}
			DotCommandResult::Continue
		}
		".token" => {
			if parts.len() == 1 {
				match current_token {
					Some(token) => println!("Current token: {}", token),
					None => println!("No token set (unauthenticated)"),
				}
				DotCommandResult::Continue
			} else {
				let new_token = parts[1..].join(" ");
				DotCommandResult::Reauthenticate(new_token)
			}
		}
		_ => {
			println!("Unknown command: {}. ValueType .help for available commands.", cmd);
			DotCommandResult::Continue
		}
	}
}

async fn execute_query(client: &mut WsClient, rql: &str, display_mode: DisplayMode) {
	let rql = rql.trim_end_matches(';').trim();

	match client.query(rql, None).await {
		Ok(result) => {
			print_query_result(&result, display_mode);
		}
		Err(e) => {
			eprintln!("Error: {}\n", e);
		}
	}
}

fn print_query_result(frames: &[Frame], display_mode: DisplayMode) {
	if frames.is_empty() {
		println!("(no results)\n");
		return;
	}

	let max_width = match display_mode {
		DisplayMode::Truncate => terminal_size().map(|(Width(w), _)| w as usize),
		DisplayMode::Full => None,
	};

	for (i, frame) in frames.iter().enumerate() {
		if frames.len() > 1 {
			println!("--- Frame {} ---", i + 1);
		}

		if let Some(width) = max_width {
			print_frame_truncated(frame, width);
		} else {
			println!("{}", frame);
		}
	}
	println!();
}

fn print_frame_truncated(frame: &Frame, max_width: usize) {
	use reifydb_client::value::util::unicode::UnicodeWidthStr;

	let row_count = frame.first().map_or(0, |c| c.data.len());
	let has_row_numbers = !frame.row_numbers().is_empty();

	let mut natural_widths: Vec<usize> = Vec::new();

	if has_row_numbers {
		let header_width = "rownum".width();
		let max_val_width = frame.row_numbers().iter().map(|rn| rn.to_string().width()).max().unwrap_or(0);
		natural_widths.push(header_width.max(max_val_width));
	}

	for col in &frame.columns {
		let header_width = col.name.width();
		let max_val_width = (0..col.data.len()).map(|i| col.data.as_string(i).width()).max().unwrap_or(0);
		natural_widths.push(header_width.max(max_val_width));
	}

	// Format: "| col1 | col2 | col3 |"
	// Each column: " content " (2 padding) + "|" separator
	let mut num_cols_to_show = 0;
	let mut current_width = 0;
	for &col_width in &natural_widths {
		let col_total = col_width + 3; // 2 padding + 1 separator
		if current_width + col_total < max_width {
			// +1 for final "|"
			current_width += col_total;
			num_cols_to_show += 1;
		} else {
			break;
		}
	}

	if num_cols_to_show == natural_widths.len() {
		println!("{}", frame);
		return;
	}

	if num_cols_to_show == 0 {
		println!("{}", frame);
		return;
	}

	let sep: String = natural_widths
		.iter()
		.take(num_cols_to_show)
		.map(|w| format!("+{}", "-".repeat(*w + 2)))
		.collect::<String>()
		+ "+";

	println!("{}", sep);

	let mut header_parts = Vec::new();
	let mut col_idx = 0;

	if has_row_numbers && col_idx < num_cols_to_show {
		let name = "rownum";
		let w = natural_widths[col_idx];
		let pad = w - name.width();
		let l = pad / 2;
		let r = pad - l;
		header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
		col_idx += 1;
	}

	for col in &frame.columns {
		if col_idx >= num_cols_to_show {
			break;
		}
		let name = &col.name;
		let w = natural_widths[col_idx];
		let pad = w - name.width();
		let l = pad / 2;
		let r = pad - l;
		header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
		col_idx += 1;
	}

	println!("|{}|", header_parts.join("|"));
	println!("{}", sep);

	for row_idx in 0..row_count {
		let mut row_parts = Vec::new();
		let mut col_idx = 0;

		if has_row_numbers && col_idx < num_cols_to_show {
			let w = natural_widths[col_idx];
			let val = if row_idx < frame.row_numbers().len() {
				frame.row_numbers()[row_idx].to_string()
			} else {
				"none".to_string()
			};
			let pad = w - val.width();
			let l = pad / 2;
			let r = pad - l;
			row_parts.push(format!(" {:l$}{}{:r$} ", "", val, ""));
			col_idx += 1;
		}

		for col in &frame.columns {
			if col_idx >= num_cols_to_show {
				break;
			}
			let w = natural_widths[col_idx];
			let val = col.data.as_string(row_idx);
			let pad = w - val.width();
			let l = pad / 2;
			let r = pad - l;
			row_parts.push(format!(" {:l$}{}{:r$} ", "", val, ""));
			col_idx += 1;
		}

		println!("|{}|", row_parts.join("|"));
	}

	println!("{}", sep);
}
