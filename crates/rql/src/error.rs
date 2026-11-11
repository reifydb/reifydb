// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error, fmt};

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::value_type::ValueType,
};

#[derive(Debug, thiserror::Error)]
pub enum RqlError {
	#[error("changes cannot run with this WITH block: {reason}")]
	ChangesWithRefused {
		reason: String,
		fragment: Fragment,
	},

	#[error("Slide must be smaller than window duration")]
	WindowSlideTooLarge {
		fragment: Fragment,
		slide_value: String,
		window_value: String,
	},

	#[error("Slide must be a positive value")]
	WindowSlideNotPositive {
		fragment: Fragment,
		window_value: String,
	},

	#[error("Incompatible slide type with window type")]
	WindowIncompatibleSlideType {
		fragment: Fragment,
		window_type: String,
		slide_type: String,
	},

	#[error("Immutable must be smaller than lateness")]
	WindowImmutableNotSmallerThanLateness {
		fragment: Fragment,
		immutable_value: String,
		lateness_value: String,
	},

	#[error("Immutable must be smaller than the window")]
	WindowImmutableNotSmallerThanWindow {
		fragment: Fragment,
		immutable_value: String,
		window_value: String,
	},

	#[error("Rolling window needs a pane")]
	WindowPaneRequired {
		fragment: Fragment,
		window_value: String,
	},

	#[error("Pane must not be larger than the window")]
	WindowPaneLargerThanWindow {
		fragment: Fragment,
		pane_value: String,
		window_value: String,
	},

	#[error("Pane must divide the window evenly")]
	WindowPaneNotDividingWindow {
		fragment: Fragment,
		pane_value: String,
		window_value: String,
	},

	#[error("Windowed view group members must have distinct durations")]
	WindowedViewGroupDuplicateDuration {
		duration: String,
		fragment: Fragment,
	},

	#[error("Windowed view group needs exactly one window")]
	WindowedViewGroupRequiresOneWindow {
		fragment: Fragment,
	},

	#[error("Windowed view group window must not set duration")]
	WindowedViewGroupWindowHasDuration {
		fragment: Fragment,
	},

	#[error("Windowed view group needs at least one member")]
	WindowedViewGroupNoMembers {
		fragment: Fragment,
	},

	#[error("Windowed view group window must not set immutable")]
	WindowedViewGroupWindowHasImmutable {
		fragment: Fragment,
	},

	#[error("Windowed view group window must not set pane")]
	WindowedViewGroupWindowHasPane {
		fragment: Fragment,
	},

	#[error("Windowed view group window must be on the main chain")]
	WindowedViewGroupWindowNotOnMainChain {
		fragment: Fragment,
	},

	#[error("Cannot mix keyed and positional rows in the same INSERT")]
	InsertMixedRowTypes {
		fragment: Fragment,
	},

	#[error("INSERT ... WITH is only supported for queues")]
	InsertWithOnlyForQueues {
		fragment: Fragment,
	},

	#[error("queue `{queue}` declares deduplicate by {{{by}}}; INSERT WITH deduplication_key is not allowed")]
	InsertWithKeyOnDeduplicatingQueue {
		fragment: Fragment,
		queue: String,
		by: String,
	},

	#[error("queue items are immutable")]
	QueueImmutable {
		fragment: Fragment,
	},

	#[error("Positional INSERT expects {expected} values, got {actual}")]
	InsertPositionalWrongLength {
		fragment: Fragment,
		expected: usize,
		actual: usize,
		column_names: Vec<String>,
	},

	#[error("BREAK can only be used inside a loop")]
	BreakOutsideLoop,

	#[error("CONTINUE can only be used inside a loop")]
	ContinueOutsideLoop,

	#[error("Multiple statements must be separated by semicolons")]
	MissingSemicolon {
		fragment: Fragment,
		count: usize,
	},

	#[error("{operator} needs an input")]
	MissingInput {
		operator: &'static str,
		fragment: Fragment,
	},

	#[error("Internal error in function {name}: {details}")]
	InternalFunctionError {
		name: String,
		fragment: Fragment,
		details: String,
	},

	#[error("`{column}` is declared as the #time populator but the object has no such column")]
	TimePopulatorUnknownColumn {
		column: String,
		available: Vec<String>,
		fragment: Fragment,
	},

	#[error("`{column}` is declared as the #time populator but is {found}, not DateTime")]
	TimePopulatorNotDateTime {
		column: String,
		found: ValueType,
		fragment: Fragment,
	},

	#[error("an object without a time domain must not declare a row ttl")]
	RowTtlWithoutTimeDomain {
		fragment: Fragment,
	},

	#[error("event-time column `{column}` cannot use a dictionary")]
	TimePopulatorDictionary {
		column: String,
		fragment: Fragment,
	},
}

impl IntoDiagnostic for RqlError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			RqlError::ChangesWithRefused { reason, fragment } => Diagnostic {
				code: "OP_004".to_string(),
				rql: None,
				message: format!("changes cannot run with this WITH block: {}", reason),
				column: None,
				fragment,
				label: Some("refused WITH block".to_string()),
				help: Some(
					"Each of insert, update and delete says what that change becomes: insert: insert|drop, update: update|insert|drop, delete: delete|insert|drop"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowSlideTooLarge { fragment, slide_value, window_value } => Diagnostic {
				code: "WINDOW_003".to_string(),
				rql: None,
				message: format!(
					"Slide ({}) must be smaller than window duration ({}) for overlapping sliding windows",
					slide_value, window_value
				),
				column: None,
				fragment,
				label: Some("slide too large".to_string()),
				help: Some(
					"Reduce the slide value to be smaller than the window size for overlapping windows".to_string(),
				),
				notes: vec![
					"Sliding windows create overlapping segments when slide < window size".to_string(),
					"If slide >= window size, consider using tumbling windows instead".to_string(),
					"Example: For 10-minute windows, use slide values like \"2m\", \"5m\", or \"1m\"".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowSlideNotPositive { fragment, window_value } => Diagnostic {
				code: "WINDOW_008".to_string(),
				rql: None,
				message: format!(
					"Slide must be greater than zero for window duration ({})",
					window_value
				),
				column: None,
				fragment,
				label: Some("slide is zero".to_string()),
				help: Some(
					"Give the slide a positive value smaller than the window size, e.g. slide: 1m for a 5m window"
						.to_string(),
				),
				notes: vec![
					"A zero slide has no defined meaning: every window would start at the same place".to_string(),
					"The slide is how far each window advances, so it must be at least one unit".to_string(),
					"For non-overlapping windows, use a tumbling window instead of a slide equal to the size".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowIncompatibleSlideType { fragment, window_type, slide_type } => Diagnostic {
				code: "WINDOW_004".to_string(),
				rql: None,
				message: format!("Incompatible slide type {} with window type {}", slide_type, window_type),
				column: None,
				fragment,
				label: Some("mismatched types".to_string()),
				help: Some(
					"Use duration-based slide for time windows, or count-based slide for count windows".to_string(),
				),
				notes: vec![
					"Time-based windows (duration) require duration-based slide parameters (e.g., 1m, 30s)".to_string(),
					"Count-based windows (count) require numeric slide parameters (e.g., 10, 50)".to_string(),
					"Example time window: WINDOW WITH { duration: 5m, slide: 1m }".to_string(),
					"Example count window: WINDOW WITH { count: 100, slide: 20 }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowImmutableNotSmallerThanLateness { fragment, immutable_value, lateness_value } => Diagnostic {
				code: "WINDOW_009".to_string(),
				rql: None,
				message: format!(
					"Immutable ({}) must be smaller than lateness ({})",
					immutable_value, lateness_value
				),
				column: None,
				fragment,
				label: Some("immutable too large".to_string()),
				help: Some(
					"Reduce immutable below lateness, or drop it to keep every row for the life of the window".to_string(),
				),
				notes: vec![
					"Lateness already bounds the window, so an immutable at or beyond it promises nothing".to_string(),
					"Immutable is how long a row may still be updated or deleted, lateness is how long a late insert is still accepted".to_string(),
					"Example: WINDOW TUMBLING { count(*) } WITH { duration: 1h, lateness: 20s, immutable: 15s }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowImmutableNotSmallerThanWindow { fragment, immutable_value, window_value } => Diagnostic {
				code: "WINDOW_010".to_string(),
				rql: None,
				message: format!(
					"Immutable ({}) must be smaller than the window ({})",
					immutable_value, window_value
				),
				column: None,
				fragment,
				label: Some("immutable too large".to_string()),
				help: Some(
					"Reduce immutable below the window duration (or gap), or drop it to keep every row for the life of the window".to_string(),
				),
				notes: vec![
					"An immutable at or beyond the window's own span can never fold anything before the window closes".to_string(),
					"Immutable is how long a row may still be updated or deleted before the accumulator folds it away".to_string(),
					"Example: WINDOW TUMBLING { count(*) } WITH { duration: 1h, immutable: 15s }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowPaneRequired { fragment, window_value } => Diagnostic {
				code: "WINDOW_011".to_string(),
				rql: None,
				message: format!("Rolling window ({}) needs a pane", window_value),
				column: None,
				fragment,
				label: Some("pane missing".to_string()),
				help: Some("Add a pane that divides the duration, e.g. pane: 1m for 60 panes in 1h".to_string()),
				notes: vec![
					"A rolling window keeps one aggregate per pane, so the pane bounds its state to duration / pane".to_string(),
					"The pane is also the step in which an event enters and leaves the window".to_string(),
					"Example: WINDOW ROLLING { count(*) } WITH { duration: 1h, pane: 1m, lateness: 20s }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowPaneLargerThanWindow { fragment, pane_value, window_value } => Diagnostic {
				code: "WINDOW_012".to_string(),
				rql: None,
				message: format!("Pane ({}) must not be larger than the window ({})", pane_value, window_value),
				column: None,
				fragment,
				label: Some("pane too large".to_string()),
				help: Some("Use a pane at most the window duration".to_string()),
				notes: vec![
					"A pane wider than the window cannot fit inside it even once".to_string(),
					"Example: WINDOW ROLLING { count(*) } WITH { duration: 1h, pane: 1m, lateness: 20s }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowPaneNotDividingWindow { fragment, pane_value, window_value } => Diagnostic {
				code: "WINDOW_013".to_string(),
				rql: None,
				message: format!("Pane ({}) must divide the window ({}) evenly", pane_value, window_value),
				column: None,
				fragment,
				label: Some("pane does not fit".to_string()),
				help: Some("Pick a pane that fits a whole number of times into the duration".to_string()),
				notes: vec![
					"A window made of whole panes enters and leaves each pane as one step".to_string(),
					"Example: WINDOW ROLLING { count(*) } WITH { duration: 3m, pane: 1m, lateness: 20s }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowedViewGroupDuplicateDuration { duration, fragment } => Diagnostic {
				code: "WINDOW_014".to_string(),
				rql: None,
				message: format!("Windowed view group members must have distinct durations; {} is given more than once", duration),
				column: None,
				fragment,
				label: Some("duration given twice".to_string()),
				help: Some("Give every member its own duration".to_string()),
				notes: vec![
					"Each member is one window duration; two members with one duration would compute the same window twice".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowedViewGroupRequiresOneWindow { fragment } => Diagnostic {
				code: "WINDOW_015".to_string(),
				rql: None,
				message: "Windowed view group needs exactly one window (a windowed apply or a window operator)".to_string(),
				column: None,
				fragment,
				label: Some("no single window".to_string()),
				help: Some("Write exactly one windowed apply or window operator in the AS block".to_string()),
				notes: vec![
					"The member durations are applied to that one window".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowedViewGroupWindowHasDuration { fragment } => Diagnostic {
				code: "WINDOW_016".to_string(),
				rql: None,
				message: "Windowed view group window must not set duration; each member sets its own".to_string(),
				column: None,
				fragment,
				label: Some("duration on the window".to_string()),
				help: Some("Remove duration from the window and set it on each member".to_string()),
				notes: vec![
					"Example: members: [ { view: ns::v1m, duration: 1m } ]".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowedViewGroupNoMembers { fragment } => Diagnostic {
				code: "WINDOW_017".to_string(),
				rql: None,
				message: "Windowed view group needs at least one member".to_string(),
				column: None,
				fragment,
				label: Some("no members".to_string()),
				help: Some("List at least one member in WITH { members: [ ... ] }".to_string()),
				notes: vec![
					"Each member names its view and its duration".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowedViewGroupWindowHasImmutable { fragment } => Diagnostic {
				code: "WINDOW_018".to_string(),
				rql: None,
				message: "Windowed view group window must not set immutable; set it on the members that need it".to_string(),
				column: None,
				fragment,
				label: Some("immutable on the window".to_string()),
				help: Some("Remove immutable from the window and set it on each member that needs it".to_string()),
				notes: vec![
					"A member without immutable keeps every row for the life of its window".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowedViewGroupWindowHasPane { fragment } => Diagnostic {
				code: "WINDOW_020".to_string(),
				rql: None,
				message: "Windowed view group window must not set pane; set it on each rolling member".to_string(),
				column: None,
				fragment,
				label: Some("pane on the window".to_string()),
				help: Some("Remove pane from the window and set it on each member".to_string()),
				notes: vec![
					"Example: members: [ { view: ns::v1h, duration: 1h, pane: 1m } ]".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowedViewGroupWindowNotOnMainChain { fragment } => Diagnostic {
				code: "WINDOW_019".to_string(),
				rql: None,
				message: "Windowed view group window must be on the main chain of the AS block, not inside a join, lookup or subquery".to_string(),
				column: None,
				fragment,
				label: Some("window off the main chain".to_string()),
				help: Some("Move the window to the top level of the AS block".to_string()),
				notes: vec![
					"The member durations are applied to the top-level window only".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertWithOnlyForQueues { fragment } => Diagnostic {
				code: "INSERT_005".to_string(),
				rql: None,
				message: "INSERT ... WITH is only supported for queues".to_string(),
				column: None,
				fragment,
				label: Some("WITH on a non-queue target".to_string()),
				help: Some(
					"Remove the WITH block, or target a queue created with CREATE QUEUE".to_string(),
				),
				notes: vec![
					"deduplication_key and not_before only have meaning for queue items".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertWithKeyOnDeduplicatingQueue { fragment, queue, by } => Diagnostic {
				code: "INSERT_008".to_string(),
				rql: None,
				message: format!(
					"queue `{}` declares deduplicate by {{{}}}; INSERT WITH deduplication_key is not allowed",
					queue, by
				),
				column: None,
				fragment,
				label: Some("deduplication_key on a deduplicating queue".to_string()),
				help: Some(
					"remove the deduplication_key option, or drop the queue's deduplicate declaration"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			RqlError::QueueImmutable { fragment } => Diagnostic {
				code: "QUEUE_001".to_string(),
				rql: None,
				message: "queue items are immutable".to_string(),
				column: None,
				fragment,
				label: Some("UPDATE and DELETE are not supported on a queue".to_string()),
				help: Some(
					"Items leave a queue by being acknowledged or by retention, not by being edited"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertMixedRowTypes { fragment } => Diagnostic {
				code: "INSERT_003".to_string(),
				rql: None,
				message: "Cannot mix keyed {key: value} and positional (value, ...) rows in the same INSERT"
					.to_string(),
				column: None,
				fragment,
				label: Some("mixed row types".to_string()),
				help: Some("Use either all keyed rows [{id: 1, name: \"Alice\"}] or all positional rows [(1, \"Alice\")] in a single INSERT".to_string()),
				notes: vec![
					"Keyed rows use curly braces: { column: value, ... }".to_string(),
					"Positional rows use parentheses: (value1, value2, ...)".to_string(),
					"All rows in a single INSERT must use the same style".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertPositionalWrongLength { fragment, expected, actual, column_names } => Diagnostic {
				code: "INSERT_004".to_string(),
				rql: None,
				message: format!(
					"Positional INSERT expects {} values (one per column), got {}",
					expected, actual
				),
				column: None,
				fragment,
				label: Some(format!("expected {} values", expected)),
				help: Some(format!(
					"Provide a value for each column in order: ({})",
					column_names.join(", ")
				)),
				notes: vec![
					format!("Table has {} columns: {}", expected, column_names.join(", ")),
					"Positional inserts must provide a value for every column".to_string(),
				],
				cause: None,
				operator_chain: None,
			},




			RqlError::BreakOutsideLoop => Diagnostic {
				code: "RUNTIME_004".to_string(),
				rql: None,
				message: "BREAK can only be used inside a loop".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Use BREAK inside a LOOP, WHILE, or FOR block".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RqlError::ContinueOutsideLoop => Diagnostic {
				code: "RUNTIME_005".to_string(),
				rql: None,
				message: "CONTINUE can only be used inside a loop".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Use CONTINUE inside a LOOP, WHILE, or FOR block".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RqlError::MissingInput { operator, fragment } => Diagnostic {
				code: "QUERY_015".to_string(),
				rql: None,
				message: format!("{} needs an input", operator),
				column: None,
				fragment,
				label: None,
				help: Some(format!("start the query with a source, e.g. `from ns::table {}`", operator)),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RqlError::MissingSemicolon { fragment, count } => Diagnostic {
				code: "SYNTAX_001".to_string(),
				rql: None,
				message: format!(
					"found {} statements where only one was expected - are you missing semicolons?",
					count,
				),
				column: None,
				fragment,
				label: Some("these statements need to be separated by semicolons".to_string()),
				help: Some("Add a semicolon `;` after each statement, e.g.:\n  RUN TEST ns::test_name;\n  RUN TESTS ns".to_string()),
				notes: vec![
					"Without semicolons, multiple statements are parsed as a single compound statement".to_string(),
					"Each RUN TEST, RUN TESTS, or other top-level command should end with `;` when followed by another statement".to_string(),
				],
				cause: None,
				operator_chain: None,
			},
			RqlError::InternalFunctionError { name, fragment, details } => Diagnostic {
				code: "FUNCTION_008".to_string(),
				rql: None,
				message: format!("Internal error in function {}: {}", name, details),
				column: None,
				fragment,
				label: Some("internal error".to_string()),
				help: Some("This is an internal error - please report this issue".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RqlError::TimePopulatorUnknownColumn { column, available, fragment } => Diagnostic {
				code: "TIME_003".to_string(),
				rql: None,
				message: format!("`{}` is declared as the #time populator but no such column exists", column),
				column: None,
				fragment,
				label: Some("unknown populator column".to_string()),
				help: Some(
					"`ts` must name a column declared on this object; #time is populated from it on every write"
						.to_string(),
				),
				notes: if available.is_empty() {
					vec!["the object declares no columns".to_string()]
				} else {
					vec![format!("available columns: {}", available.join(", "))]
				},
				cause: None,
				operator_chain: None,
			},
			RqlError::TimePopulatorNotDateTime { column, found, fragment } => Diagnostic {
				code: "TIME_004".to_string(),
				rql: None,
				message: format!("`{}` is declared as the #time populator but is {}", column, found),
				column: None,
				fragment,
				label: Some("populator is not a DateTime".to_string()),
				help: Some(
					"#time is a timestamp, so the column it is populated from must be declared DateTime"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RqlError::RowTtlWithoutTimeDomain { fragment } => Diagnostic {
				code: "TIME_005".to_string(),
				rql: None,
				message: "an object without a time domain must not declare a row ttl".to_string(),
				column: None,
				fragment,
				label: Some("row ttl on a time-less object".to_string()),
				help: Some(
					"declare `time: event(<column>)` or `time: processing`, or drop the row ttl"
						.to_string(),
				),
				notes: vec![
					"an undeclared object is `time: none`, which carries no #time at all".to_string(),
				],
				cause: None,
				operator_chain: None,
			},
			RqlError::TimePopulatorDictionary { column, fragment } => Diagnostic {
				code: "TIME_006".to_string(),
				rql: None,
				message: format!("event-time column `{}` cannot use a dictionary", column),
				column: None,
				fragment,
				label: Some("populator uses a dictionary".to_string()),
				help: Some(
					"#time is read from the stored value, so its column must hold the DateTime itself, not a dictionary id"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<RqlError> for Error {
	fn from(err: RqlError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

#[derive(Debug, Clone)]
pub enum IdentifierError {
	SourceNotFound(ObjectNotFoundError),
	RemoteNamespace {
		namespace: String,
		name: String,
		address: String,
		fragment: Fragment,
	},
}

impl fmt::Display for IdentifierError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			IdentifierError::SourceNotFound(e) => {
				write!(f, "{}", e)
			}
			IdentifierError::RemoteNamespace {
				namespace,
				name,
				address,
				..
			} => {
				write!(
					f,
					"Remote namespace '{}': source '{}' is on remote instance at {}",
					namespace, name, address
				)
			}
		}
	}
}

impl error::Error for IdentifierError {}

impl From<IdentifierError> for Error {
	fn from(err: IdentifierError) -> Self {
		match err {
			IdentifierError::SourceNotFound(ref e) => CatalogError::NotFound {
				kind: CatalogObjectKind::Table,
				namespace: e.namespace.clone(),
				name: e.name.clone(),
				fragment: e.fragment.clone(),
			}
			.into(),
			IdentifierError::RemoteNamespace {
				namespace,
				name,
				address,
				fragment,
			} => {
				let notes = vec![
					format!("Namespace '{}' is configured as a remote namespace", namespace),
					format!("Remote gRPC address: {}", address),
				];
				Error(Box::new(Diagnostic {
					code: "REMOTE_001".to_string(),
					rql: None,
					message: format!(
						"Remote namespace '{}': source '{}' is on remote instance at {}",
						namespace, name, address
					),
					column: None,
					fragment,
					label: Some("source is on a remote instance".to_string()),
					help: Some(
						"Remote namespaces cannot be queried directly. Use the remote instance's endpoint instead."
							.to_string(),
					),
					notes,
					cause: None,
					operator_chain: None,
				}))
			}
		}
	}
}

#[derive(Debug, Clone)]
pub struct ObjectNotFoundError {
	pub namespace: String,
	pub name: String,
	pub fragment: Fragment,
}

impl fmt::Display for ObjectNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.namespace == "public" || self.namespace.is_empty() {
			write!(f, "Table or view '{}' does not exist", self.name)
		} else {
			write!(f, "Table or view '{}::{}' does not exist", self.namespace, self.name)
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_source_not_found_display() {
		let err = ObjectNotFoundError {
			namespace: "public".to_string(),
			name: "users".to_string(),
			fragment: Fragment::None,
		};
		assert_eq!(err.to_string(), "Table or view 'users' does not exist");

		let err = ObjectNotFoundError {
			namespace: "myshape".to_string(),
			name: "users".to_string(),
			fragment: Fragment::None,
		};
		assert_eq!(err.to_string(), "Table or view 'myshape::users' does not exist");
	}
}
