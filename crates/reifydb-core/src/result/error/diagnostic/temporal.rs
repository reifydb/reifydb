// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{IntoOwnedSpan, result::error::diagnostic::Diagnostic};

pub fn invalid_date_format(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"expected YYYY-MM-DD format, found '{}'",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_001".to_string(),
		statement: None,
		message: "invalid date format".to_string(),
		span: Some(owned_span),
		label,
		help: Some("use the format YYYY-MM-DD (e.g., 2024-03-15)"
			.to_string()),
		notes: vec![
			"dates must have exactly 3 parts separated by hyphens"
				.to_string(),
		],
		column: None,
		cause: None,
	}
}

pub fn invalid_datetime_format(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"expected YYYY-MM-DDTHH:MM:SS format, found '{}'",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_002".to_string(),
        statement: None,
        message: "invalid datetime format".to_string(),
        span: Some(owned_span),
        label,
        help: Some(
            "use the format YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM] (e.g., 2024-03-15T14:30:45)"
                .to_string(),
        ),
        notes: vec!["datetime must contain 'T' separator between date and time parts".to_string()],
        column: None,
        cause: None,
    }
}

pub fn invalid_time_format(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"expected HH:MM:SS format, found '{}'",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_003".to_string(),
        statement: None,
        message: "invalid time format".to_string(),
        span: Some(owned_span),
        label,
        help: Some("use the format HH:MM:SS[.fff][Z|±HH:MM] (e.g., 14:30:45)".to_string()),
        notes: vec!["time must have exactly 3 parts separated by colons".to_string()],
        column: None,
        cause: None,
    }
}

pub fn invalid_interval_format(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"expected P[n]Y[n]M[n]W[n]D[T[n]H[n]M[n]S] format, found '{}'",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_004".to_string(),
        statement: None,
        message: "invalid interval format".to_string(),
        span: Some(owned_span),
        label,
        help: Some(
            "use ISO 8601 duration format starting with 'P' (e.g., P1D, PT2H30M, P1Y2M3DT4H5M6S)"
                .to_string(),
        ),
        notes: vec![
            "interval must start with 'P' followed by duration components".to_string(),
            "date part: P[n]Y[n]M[n]W[n]D (years, months, weeks, days)".to_string(),
            "time part: T[n]H[n]M[n]S (hours, minutes, seconds)".to_string(),
        ],
        column: None,
        cause: None,
    }
}

pub fn invalid_year(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"year '{}' cannot be parsed as a number",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_005".to_string(),
		statement: None,
		message: format!(
			"invalid year value '{}'",
			owned_span.fragment
		),
		span: Some(owned_span),
		label,
		help: Some(
			"ensure the year is a valid 4-digit number".to_string()
		),
		notes: vec!["valid examples: 2024, 1999, 2000".to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_month(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"month '{}' cannot be parsed as a number (expected 1-12)",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_006".to_string(),
		statement: None,
		message: format!(
			"invalid month value '{}'",
			owned_span.fragment
		),
		span: Some(owned_span),
		label,
		help: Some(
			"ensure the month is a valid number between 1 and 12"
				.to_string(),
		),
		notes: vec!["valid examples: 01, 03, 12".to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_day(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"day '{}' cannot be parsed as a number (expected 1-31)",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_007".to_string(),
		statement: None,
		message: format!("invalid day value '{}'", owned_span.fragment),
		span: Some(owned_span),
		label,
		help: Some("ensure the day is a valid number between 1 and 31"
			.to_string()),
		notes: vec!["valid examples: 01, 15, 31".to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_hour(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"hour '{}' cannot be parsed as a number (expected 0-23)",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_008".to_string(),
        statement: None,
        message: format!("invalid hour value '{}'", owned_span.fragment),
        span: Some(owned_span),
        label,
        help: Some(
            "ensure the hour is a valid number between 0 and 23 (use 24-hour format)".to_string(),
        ),
        notes: vec![
            "valid examples: 09, 14, 23".to_string(),
            "hours must be in 24-hour format (00-23)".to_string(),
        ],
        column: None,
        cause: None,
    }
}

pub fn invalid_minute(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"minute '{}' cannot be parsed as a number (expected 0-59)",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_009".to_string(),
		statement: None,
		message: format!(
			"invalid minute value '{}'",
			owned_span.fragment
		),
		span: Some(owned_span),
		label,
		help: Some(
			"ensure the minute is a valid number between 0 and 59"
				.to_string(),
		),
		notes: vec!["valid examples: 00, 30, 59".to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_second(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"second '{}' cannot be parsed as a number (expected 0-59)",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_010".to_string(),
		statement: None,
		message: format!(
			"invalid second value '{}'",
			owned_span.fragment
		),
		span: Some(owned_span),
		label,
		help: Some(
			"ensure the second is a valid number between 0 and 59"
				.to_string(),
		),
		notes: vec!["valid examples: 00, 30, 59".to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_fractional_seconds(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"fractional seconds '{}' cannot be parsed as a number",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_011".to_string(),
		statement: None,
		message: format!(
			"invalid fractional seconds value '{}'",
			owned_span.fragment
		),
		span: Some(owned_span),
		label,
		help: Some("ensure fractional seconds contain only digits"
			.to_string()),
		notes: vec!["valid examples: 123, 999999, 000001".to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_date_values(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"date '{}' represents an invalid calendar date",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_012".to_string(),
        statement: None,
        message: "invalid date values".to_string(),
        span: Some(owned_span),
        label,
        help: Some("ensure the date exists in the calendar (e.g., no February 30)".to_string()),
        notes: vec![
            "check month has correct number of days".to_string(),
            "consider leap years for February 29".to_string(),
        ],
        column: None,
        cause: None,
    }
}

pub fn invalid_time_values(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"time '{}' contains out-of-range values",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_013".to_string(),
		statement: None,
		message: "invalid time values".to_string(),
		span: Some(owned_span),
		label,
		help: Some(
			"ensure hours are 0-23, minutes and seconds are 0-59"
				.to_string(),
		),
		notes: vec!["use 24-hour format for hours".to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_interval_character(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"character '{}' is not valid in ISO 8601 duration",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_014".to_string(),
        statement: None,
        message: format!("invalid character in interval '{}'", owned_span.fragment),
        span: Some(owned_span),
        label,
        help: Some("use only valid duration units: Y, M, W, D, H, m, S".to_string()),
        notes: vec![
            "date part units: Y (years), M (months), W (weeks), D (days)".to_string(),
            "time part units: H (hours), m (minutes), S (seconds)".to_string(),
        ],
        column: None,
        cause: None,
    }
}

pub fn incomplete_interval_specification(
	span: impl IntoOwnedSpan,
) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"number '{}' is missing a unit specifier",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_015".to_string(),
        statement: None,
        message: "incomplete interval specification".to_string(),
        span: Some(owned_span),
        label,
        help: Some("add a unit letter after the number (Y, M, W, D, H, M, or S)".to_string()),
        notes: vec!["example: P1D (not P1), PT2H (not PT2)".to_string()],
        column: None,
        cause: None,
    }
}

pub fn invalid_unit_in_context(
	span: impl IntoOwnedSpan,
	unit: char,
	in_time_part: bool,
) -> Diagnostic {
	let owned_span = span.into_span();
	let context = if in_time_part {
		"time part (after T)"
	} else {
		"date part (before T)"
	};
	let allowed = if in_time_part {
		"H, M, S"
	} else {
		"Y, M, W, D"
	};
	let label = Some(format!(
		"unit '{}' is not allowed in the {}",
		unit, context
	));
	Diagnostic {
		code: "TEMPORAL_016".to_string(),
		statement: None,
		message: format!("invalid unit '{}' in {}", unit, context),
		span: Some(owned_span),
		label,
		help: Some(format!("use only {} in the {}", allowed, context)),
		notes: vec![
			"date part (before T): Y, M, W, D".to_string(),
			"time part (after T): H, M, S".to_string(),
		],
		column: None,
		cause: None,
	}
}

pub fn invalid_interval_component_value(
	span: impl IntoOwnedSpan,
	unit: char,
) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"{} value '{}' cannot be parsed as a number",
		unit_name(unit),
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_017".to_string(),
		statement: None,
		message: format!(
			"invalid {} value '{}'",
			unit_name(unit),
			owned_span.fragment
		),
		span: Some(owned_span),
		label,
		help: Some(format!(
			"ensure the {} value is a valid number",
			unit_name(unit)
		)),
		notes: vec![format!("valid examples: P1{}, P10{}", unit, unit)],
		column: None,
		cause: None,
	}
}

pub fn unrecognized_temporal_pattern(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"value '{}' does not match any temporal format",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_018".to_string(),
		statement: None,
		message: "unrecognized temporal pattern".to_string(),
		span: Some(owned_span),
		label,
		help: Some("use one of the supported formats: date (YYYY-MM-DD), time (HH:MM:SS), datetime (YYYY-MM-DDTHH:MM:SS), or interval (P...)".to_string()),
		notes: vec![
			"date: 2024-03-15".to_string(),
			"time: 14:30:45".to_string(),
			"datetime: 2024-03-15T14:30:45".to_string(),
			"interval: P1Y2M3DT4H5M6S".to_string(),
		],
		column: None,
        cause: None,
	}
}

pub fn empty_date_component(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"date component '{}' is empty",
		owned_span.fragment
	));
	Diagnostic {
		code: "TEMPORAL_019".to_string(),
		statement: None,
		message: "empty date component".to_string(),
		span: Some(owned_span),
		label,
		help: Some(
			"ensure all date parts (year, month, day) are provided"
				.to_string(),
		),
		notes: vec!["date format: YYYY-MM-DD (e.g., 2024-03-15)"
			.to_string()],
		column: None,
		cause: None,
	}
}

pub fn empty_time_component(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"time component '{}' is empty",
		owned_span.fragment
	));
	Diagnostic {
        code: "TEMPORAL_020".to_string(),
        statement: None,
        message: "empty time component".to_string(),
        span: Some(owned_span),
        label,
        help: Some("ensure all time parts (hour, minute, second) are provided".to_string()),
        notes: vec!["time format: HH:MM:SS (e.g., 14:30:45)".to_string()],
        column: None,
        cause: None,
    }
}

fn unit_name(unit: char) -> &'static str {
	match unit {
		'Y' => "year",
		'M' => "month/minute",
		'W' => "week",
		'D' => "day",
		'H' => "hour",
		'S' => "second",
		_ => "unit",
	}
}
