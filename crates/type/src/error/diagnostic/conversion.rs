use crate::{Fragment, error::diagnostic::Diagnostic};

/// Array conversion error
pub fn array_conversion_error(err: std::array::TryFromSliceError) -> Diagnostic {
	Diagnostic {
		code: "CONV_001".to_string(),
		statement: None,
		message: format!("Array conversion error: {}", err),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check array size requirements".to_string()),
		notes: vec![],
		cause: None,
	}
}

/// UTF-8 conversion error
pub fn utf8_conversion_error(err: std::string::FromUtf8Error) -> Diagnostic {
	Diagnostic {
		code: "CONV_002".to_string(),
		statement: None,
		message: format!("UTF-8 conversion error: {}", err),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check string encoding".to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Integer conversion error
pub fn integer_conversion_error(err: std::num::TryFromIntError) -> Diagnostic {
	Diagnostic {
		code: "CONV_003".to_string(),
		statement: None,
		message: format!("Integer conversion error: {}", err),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check integer range limits".to_string()),
		notes: vec![],
		cause: None,
	}
}
