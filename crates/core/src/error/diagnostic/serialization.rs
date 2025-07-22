// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::error::diagnostic::Diagnostic;

/// General deserialization error
pub fn deserialization_error(msg: String) -> Diagnostic {
    Diagnostic {
        code: "SER_001".to_string(),
        statement: None,
        message: format!("Deserialization error: {}", msg),
        column: None,
        span: None,
        label: None,
        help: Some("Check data format and structure".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// General serialization error
pub fn serialization_error(msg: String) -> Diagnostic {
    Diagnostic {
        code: "SER_002".to_string(),
        statement: None,
        message: format!("Serialization error: {}", msg),
        column: None,
        span: None,
        label: None,
        help: Some("Check data format and structure".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Bincode encoding error
pub fn bincode_encode_error(err: bincode::error::EncodeError) -> Diagnostic {
    Diagnostic {
        code: "SER_003".to_string(),
        statement: None,
        message: format!("Bincode encode error: {}", err),
        column: None,
        span: None,
        label: None,
        help: Some("Check binary data format".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Bincode decoding error
pub fn bincode_decode_error(err: bincode::error::DecodeError) -> Diagnostic {
    Diagnostic {
        code: "SER_004".to_string(),
        statement: None,
        message: format!("Bincode decode error: {}", err),
        column: None,
        span: None,
        label: None,
        help: Some("Check binary data format".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Keycode-specific serialization error
pub fn keycode_serialization_error(msg: String) -> Diagnostic {
    Diagnostic {
        code: "SER_005".to_string(),
        statement: None,
        message: format!("Keycode serialization error: {}", msg),
        column: None,
        span: None,
        label: None,
        help: Some("Check keycode data and format".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Serde deserialization error
pub fn serde_deserialize_error(msg: String) -> Diagnostic {
    Diagnostic {
        code: "SERDE_001".to_string(),
        statement: None,
        message: format!("Serde deserialization error: {}", msg),
        column: None,
        span: None,
        label: None,
        help: Some("Check data format and structure".to_string()),
        notes: vec![],
        cause: None,
    }
}

/// Serde serialization error
pub fn serde_serialize_error(msg: String) -> Diagnostic {
    Diagnostic {
        code: "SERDE_002".to_string(),
        statement: None,
        message: format!("Serde serialization error: {}", msg),
        column: None,
        span: None,
        label: None,
        help: Some("Check data format and structure".to_string()),
        notes: vec![],
        cause: None,
    }
}