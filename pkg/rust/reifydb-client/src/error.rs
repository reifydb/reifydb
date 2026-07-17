// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error, fmt};

use reifydb_value::error::{Diagnostic, Error};

#[derive(Debug)]
pub enum ClientError {
	ConnectionLost,

	Timeout,

	NotAuthenticated(String),

	UnsupportedWireFormat(String),

	UnexpectedResponse(String),

	Decode(String),

	Transport(String),

	Server(Box<Diagnostic>),
}

impl fmt::Display for ClientError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ClientError::ConnectionLost => write!(f, "Connection lost"),
			ClientError::Timeout => write!(f, "Operation timed out"),
			ClientError::NotAuthenticated(msg) => write!(f, "Authentication failed: {}", msg),
			ClientError::UnsupportedWireFormat(msg) => write!(f, "{}", msg),
			ClientError::UnexpectedResponse(msg) => write!(f, "{}", msg),
			ClientError::Decode(msg) => write!(f, "{}", msg),
			ClientError::Transport(msg) => write!(f, "{}", msg),
			ClientError::Server(diagnostic) => write!(f, "{}", diagnostic.message),
		}
	}
}

impl error::Error for ClientError {}

impl From<ClientError> for Error {
	fn from(err: ClientError) -> Self {
		let diagnostic = match err {
			ClientError::Server(diagnostic) => return Error(diagnostic),
			ClientError::ConnectionLost => diag("CONNECTION_LOST", "Connection lost".to_string()),
			ClientError::Timeout => diag("TIMEOUT", "Operation timed out".to_string()),
			ClientError::NotAuthenticated(msg) => diag("AUTH_FAILED", msg),
			ClientError::UnsupportedWireFormat(msg) => diag("INVALID_FORMAT", msg),
			ClientError::UnexpectedResponse(msg) => diag("UNEXPECTED_RESPONSE", msg),
			ClientError::Decode(msg) => diag("DECODE", msg),
			ClientError::Transport(msg) => diag("TRANSPORT", msg),
		};
		Error(Box::new(diagnostic))
	}
}

impl From<Error> for ClientError {
	fn from(err: Error) -> Self {
		ClientError::Server(err.0)
	}
}

fn diag(code: &str, message: String) -> Diagnostic {
	Diagnostic {
		code: code.to_string(),
		message,
		..Default::default()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::error::{Diagnostic, Error};

	use super::ClientError;

	#[test]
	fn maps_variants_to_stable_diagnostic_codes() {
		// Callers and the wire_format_validation test match on these codes; they must not
		// drift when a variant is added or reordered.
		let cases = [
			(ClientError::ConnectionLost, "CONNECTION_LOST", "Connection lost"),
			(ClientError::Timeout, "TIMEOUT", "Operation timed out"),
			(ClientError::NotAuthenticated("bad token".to_string()), "AUTH_FAILED", "bad token"),
			(
				ClientError::UnsupportedWireFormat("WireFormat::Json is not supported".to_string()),
				"INVALID_FORMAT",
				"WireFormat::Json is not supported",
			),
			(ClientError::UnexpectedResponse("weird".to_string()), "UNEXPECTED_RESPONSE", "weird"),
			(ClientError::Decode("bad bytes".to_string()), "DECODE", "bad bytes"),
			(ClientError::Transport("socket died".to_string()), "TRANSPORT", "socket died"),
		];

		for (err, expected_code, expected_message) in cases {
			let error: Error = err.into();
			assert_eq!(error.0.code, expected_code);
			assert_eq!(error.0.message, expected_message);
		}
	}

	#[test]
	fn server_variant_passes_the_diagnostic_through_unchanged() {
		// A server-relayed diagnostic carries fields (help, notes, ...) the category variants
		// never set; wrapping and unwrapping must preserve the original untouched, not rebuild
		// a fresh Diagnostic from only code + message.
		let original = Diagnostic {
			code: "SOME_SERVER_CODE".to_string(),
			message: "server said no".to_string(),
			help: Some("try again".to_string()),
			notes: vec!["note one".to_string()],
			..Default::default()
		};

		let round_tripped: Error = ClientError::from(Error(Box::new(original.clone()))).into();

		assert_eq!(*round_tripped.0, original);
	}
}
