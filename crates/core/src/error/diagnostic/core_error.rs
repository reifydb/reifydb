// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, IntoDiagnostic, util::value_max},
	fragment::Fragment,
};

use crate::error::{
	CoreError,
	diagnostic::flow::{
		flow_operator_retention_required, flow_operator_rolling_unbounded, flow_operator_timer_kind_reserved,
		flow_operator_with_count_span, flow_operator_with_duration_span,
		flow_operator_with_immutable_not_below_lateness, flow_operator_with_not_accepted,
		flow_operator_with_pane_missing, flow_operator_with_retention_below_lateness,
		flow_operator_with_retention_not_supported, flow_operator_with_session_lateness,
		flow_operator_with_session_zero_gap, flow_operator_with_window_kind_unsupported,
		flow_operator_with_window_missing, flow_operator_with_window_not_supported,
		flow_operator_with_window_size_count, flow_operator_with_window_size_duration,
		flow_operator_with_window_slide_count, flow_operator_with_window_slide_duration,
	},
};

impl IntoDiagnostic for CoreError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			CoreError::Type(err) => err.into_diagnostic(),

			CoreError::IndexVariableLengthNotSupported => Diagnostic {
				code: "CA_009".to_string(),
				rql: None,
				message: "variable-length types (UTF8, BLOB) are not supported in indexes".to_string(),
				fragment: Fragment::None,
				label: Some("unsupported type for indexing".to_string()),
				help: Some("only fixed-size types can be indexed currently".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::IndexTypesDirectionsMismatch {
				types_len,
				directions_len,
			} => Diagnostic {
				code: "CA_010".to_string(),
				rql: None,
				message: format!(
					"mismatch between number of types ({}) and directions ({})",
					types_len, directions_len
				),
				fragment: Fragment::None,
				label: Some("length mismatch".to_string()),
				help: Some("each indexed field must have a corresponding sort direction".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::FrameError {
				message,
			} => Diagnostic {
				code: "ENG_001".to_string(),
				rql: None,
				message: format!("Frame processing error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check frame data and operations".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::AppendSystemColumnMismatch {
				column,
			} => Diagnostic {
				code: "ENG_008".to_string(),
				rql: None,
				message: format!(
					"cannot append rows: '{}' is present on one side but not the other",
					column
				),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("pass row numbers for every append, or for none".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},






			CoreError::OperatorWithCountSpan {
				key,
				count,
			} => flow_operator_with_count_span(key, count),

			CoreError::OperatorWithImmutableNotBelowLateness {
				immutable,
				lateness,
			} => flow_operator_with_immutable_not_below_lateness(immutable, lateness),

			CoreError::OperatorWithWindowMissing => flow_operator_with_window_missing(),

			CoreError::OperatorWithPaneMissing => flow_operator_with_pane_missing(),

			CoreError::OperatorRollingUnbounded {
				operator,
			} => flow_operator_rolling_unbounded(operator),

			CoreError::OperatorWithWindowKindUnsupported {
				kind,
				supported,
			} => flow_operator_with_window_kind_unsupported(kind, &supported),

			CoreError::OperatorWithWindowNotSupported {
				kind,
			} => flow_operator_with_window_not_supported(kind),

			CoreError::OperatorWithWindowSizeCount {
				count,
			} => flow_operator_with_window_size_count(count),

			CoreError::OperatorWithWindowSizeDuration {
				size,
			} => flow_operator_with_window_size_duration(size),

			CoreError::OperatorWithWindowSlideCount {
				count,
			} => flow_operator_with_window_slide_count(count),

			CoreError::OperatorWithWindowSlideDuration {
				slide,
			} => flow_operator_with_window_slide_duration(slide),

			CoreError::OperatorWithSessionLateness {
				lateness,
			} => flow_operator_with_session_lateness(lateness),

			CoreError::OperatorWithSessionZeroGap => flow_operator_with_session_zero_gap(),

			CoreError::OperatorWithDurationSpan {
				key,
				duration,
			} => flow_operator_with_duration_span(key, duration),

			CoreError::OperatorWithNotAccepted => flow_operator_with_not_accepted(),

			CoreError::OperatorRetentionRequired => flow_operator_retention_required(),

			CoreError::OperatorWithRetentionNotSupported => flow_operator_with_retention_not_supported(),

			CoreError::OperatorWithRetentionBelowLateness {
				retention,
				lateness,
			} => flow_operator_with_retention_below_lateness(retention, lateness),

			CoreError::OperatorTimerKindReserved {
				kind,
			} => flow_operator_timer_kind_reserved(kind),


			CoreError::PrimaryKeyDigestColumn {
				fragment,
				column,
				ty,
			} => Diagnostic {
				code: "INDEX_003".to_string(),
				rql: None,
				message: format!("cannot use column `{}` in a primary key: a {} value cannot be a key", column, ty),
				column: None,
				fragment,
				label: Some("digest primary key column".to_string()),
				help: Some(
					"a digest has no key encoding and no equality, so it cannot identify a row; key the table by a column of another type"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::DigestWriteTypeMismatch {
				fragment,
				expected,
				actual,
			} => Diagnostic {
				code: "CONSTRAINT_008".to_string(),
				rql: None,
				message: format!("expected {}, got {}", expected, actual),
				column: None,
				fragment,
				label: Some(format!("the column holds {}", expected)),
				help: Some(
					"a digest is written only into a digest column of the same input type and accuracy, and such a column takes nothing else"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},



			CoreError::SequenceExhausted {
				value_type,
			} => {
				let max = value_max(value_type.clone());
				Diagnostic {
					code: "SEQUENCE_001".to_string(),
					rql: None,
					message: format!("sequence generator of type `{}` is exhausted", value_type),
					fragment: Fragment::None,
					label: Some("no more values can be generated".to_string()),
					help: Some(format!("maximum value for `{}` is `{}`", value_type, max)),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}


			CoreError::SubsystemInitFailed {
				subsystem,
				reason,
			} => Diagnostic {
				code: "SUB_001".to_string(),
				rql: None,
				message: format!("{} subsystem initialization failed: {}", subsystem, reason),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check subsystem configuration and dependencies".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::SubsystemFeatureDisabled {
				feature,
			} => Diagnostic {
				code: "SUB_002".to_string(),
				rql: None,
				message: format!("Required feature '{}' is not enabled", feature),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Enable the required feature in Cargo.toml".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CoreError::SubsystemBindFailed {
				addr,
				reason,
			} => Diagnostic {
				code: "SUB_003".to_string(),
				rql: None,
				message: format!("Failed to bind to {}: {}", addr, reason),
				column: None,
				fragment: Fragment::None,
				label: Some("Check if address is already in use or permissions are insufficient"
					.to_string()),
				help: Some("Try a different port or check firewall settings".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},


			CoreError::SubsystemAddressUnavailable {
				reason,
			} => Diagnostic {
				code: "SUB_005".to_string(),
				rql: None,
				message: format!("Failed to get local address: {}", reason),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

		}
	}
}
