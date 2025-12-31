// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{Display, Formatter},
	ops::{Deref, DerefMut},
};

use serde::{de, ser};

pub mod diagnostic;
mod r#macro;

use diagnostic::{Diagnostic, conversion, render::DefaultRenderer};

#[derive(Debug, PartialEq)]
pub struct Error(pub Diagnostic);

impl Deref for Error {
	type Target = Diagnostic;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Error {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl Display for Error {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let out = DefaultRenderer::render_string(&self.0);
		f.write_str(out.as_str())
	}
}

impl Error {
	pub fn diagnostic(self) -> Diagnostic {
		self.0
	}
}

impl std::error::Error for Error {}

impl de::Error for Error {
	fn custom<T: Display>(msg: T) -> Self {
		crate::error!(diagnostic::serde::serde_deserialize_error(msg.to_string()))
	}
}

impl ser::Error for Error {
	fn custom<T: Display>(msg: T) -> Self {
		crate::error!(diagnostic::serde::serde_serialize_error(msg.to_string()))
	}
}

impl From<std::num::TryFromIntError> for Error {
	fn from(err: std::num::TryFromIntError) -> Self {
		crate::error!(conversion::integer_conversion_error(err))
	}
}

impl From<std::array::TryFromSliceError> for Error {
	fn from(err: std::array::TryFromSliceError) -> Self {
		crate::error!(conversion::array_conversion_error(err))
	}
}

impl From<std::string::FromUtf8Error> for Error {
	fn from(err: std::string::FromUtf8Error) -> Self {
		crate::error!(conversion::utf8_conversion_error(err))
	}
}
