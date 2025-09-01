// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod blocking;
mod callback;
mod channel;

use std::time::Instant;

pub use blocking::BlockingSession;
pub use callback::CallbackSession;
pub use channel::ChannelSession;

use crate::{Response, ResponsePayload, WebsocketFrame};

/// Response message for channel sessions
#[derive(Debug)]
pub struct ResponseMessage {
	pub request_id: String,
	pub response: Result<Response, String>,
	pub timestamp: Instant,
}

/// Result type for command operations
#[derive(Debug)]
pub struct CommandResult {
	pub frames: Vec<DataFrame>,
	pub affected_rows: Option<usize>,
	pub execution_time: Duration,
}

/// Result type for query operations
#[derive(Debug)]
pub struct QueryResult {
	pub frames: Vec<DataFrame>,
	pub rows_returned: usize,
	pub execution_time: Duration,
}

/// Represents a data frame from the database
#[derive(Debug, Clone)]
pub struct DataFrame {
	pub name: String,
	pub columns: Vec<Column>,
	pub rows: Vec<Row>,
}

/// Represents a column in a data frame
#[derive(Debug, Clone)]
pub struct Column {
	pub name: String,
	pub data_type: String,
}

/// Represents a row of data
#[derive(Debug, Clone)]
pub struct Row {
	pub values: Vec<String>,
}

use std::time::Duration;

// Helper functions for parsing responses
pub(crate) fn parse_command_response(
	response: Response,
) -> Result<CommandResult, String> {
	match response.payload {
		ResponsePayload::Command(cmd_response) => {
			Ok(CommandResult {
				frames: convert_frames(cmd_response.frames),
				affected_rows: None, /* Could be extracted
				                      * from frames if
				                      * available */
				execution_time: Duration::from_millis(0), /* Would need timing info */
			})
		}
		ResponsePayload::Err(err) => {
			Err(format!("Command error: {:?}", err.diagnostic))
		}
		_ => Err("Unexpected response type for command".to_string()),
	}
}

pub(crate) fn parse_query_response(
	response: Response,
) -> Result<QueryResult, String> {
	match response.payload {
		ResponsePayload::Query(query_response) => {
			let frames = convert_frames(query_response.frames);
			let rows_returned =
				frames.iter().map(|f| f.rows.len()).sum();

			Ok(QueryResult {
				frames,
				rows_returned,
				execution_time: Duration::from_millis(0), /* Would need timing info */
			})
		}
		ResponsePayload::Err(err) => {
			Err(format!("Query error: {:?}", err.diagnostic))
		}
		_ => Err("Unexpected response type for query".to_string()),
	}
}

fn convert_frames(ws_frames: Vec<WebsocketFrame>) -> Vec<DataFrame> {
	ws_frames
		.into_iter()
		.map(|frame| {
			let columns: Vec<Column> = frame
				.columns
				.iter()
				.map(|col| Column {
					name: col.name.clone(),
					data_type: format!("{:?}", col.r#type),
				})
				.collect();

			// Convert columnar data to row format
			let mut rows = Vec::new();
			if !frame.columns.is_empty() {
				let num_rows = frame.columns[0].data.len();
				for row_idx in 0..num_rows {
					let mut row_values = Vec::new();
					for col in &frame.columns {
						if row_idx < col.data.len() {
							row_values.push(col
								.data[row_idx]
								.clone());
						} else {
							row_values.push("NULL"
								.to_string());
						}
					}
					rows.push(Row {
						values: row_values,
					});
				}
			}

			DataFrame {
				name: frame.name,
				columns,
				rows,
			}
		})
		.collect()
}
