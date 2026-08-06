// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum Color {
	Black,
	Red,
	Green,
	Yellow,
	Blue,
	Magenta,
	Cyan,
	White,
	BrightBlack,
	BrightRed,
	BrightGreen,
	BrightYellow,
	BrightBlue,
	BrightMagenta,
	BrightCyan,
	BrightWhite,
}

impl Color {
	fn foreground_code(&self) -> &'static str {
		match self {
			Color::Black => "\x1b[30m",
			Color::Red => "\x1b[31m",
			Color::Green => "\x1b[32m",
			Color::Yellow => "\x1b[33m",
			Color::Blue => "\x1b[34m",
			Color::Magenta => "\x1b[35m",
			Color::Cyan => "\x1b[36m",
			Color::White => "\x1b[37m",
			Color::BrightBlack => "\x1b[90m",
			Color::BrightRed => "\x1b[91m",
			Color::BrightGreen => "\x1b[92m",
			Color::BrightYellow => "\x1b[93m",
			Color::BrightBlue => "\x1b[94m",
			Color::BrightMagenta => "\x1b[95m",
			Color::BrightCyan => "\x1b[96m",
			Color::BrightWhite => "\x1b[97m",
		}
	}

	fn background_code(&self) -> &'static str {
		match self {
			Color::Black => "\x1b[40m",
			Color::Red => "\x1b[41m",
			Color::Green => "\x1b[42m",
			Color::Yellow => "\x1b[43m",
			Color::Blue => "\x1b[44m",
			Color::Magenta => "\x1b[45m",
			Color::Cyan => "\x1b[46m",
			Color::White => "\x1b[47m",
			Color::BrightBlack => "\x1b[100m",
			Color::BrightRed => "\x1b[101m",
			Color::BrightGreen => "\x1b[102m",
			Color::BrightYellow => "\x1b[103m",
			Color::BrightBlue => "\x1b[104m",
			Color::BrightMagenta => "\x1b[105m",
			Color::BrightCyan => "\x1b[106m",
			Color::BrightWhite => "\x1b[107m",
		}
	}
}

pub struct ColoredString {
	text: String,
	foreground: Option<Color>,
	background: Option<Color>,
	bold: bool,
	dimmed: bool,
	italic: bool,
	underline: bool,
}

impl ColoredString {
	fn new(text: impl Into<String>) -> Self {
		Self {
			text: text.into(),
			foreground: None,
			background: None,
			bold: false,
			dimmed: false,
			italic: false,
			underline: false,
		}
	}
}

impl fmt::Display for ColoredString {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut codes = Vec::new();

		if self.bold {
			codes.push("\x1b[1m");
		}
		if self.dimmed {
			codes.push("\x1b[2m");
		}
		if self.italic {
			codes.push("\x1b[3m");
		}
		if self.underline {
			codes.push("\x1b[4m");
		}

		if let Some(fg) = self.foreground {
			codes.push(fg.foreground_code());
		}

		if let Some(bg) = self.background {
			codes.push(bg.background_code());
		}

		for code in codes {
			write!(f, "{}", code)?;
		}

		write!(f, "{}", self.text)?;

		write!(f, "\x1b[0m")
	}
}

pub trait Colorize: Sized {
	fn red(self) -> ColoredString;
	fn green(self) -> ColoredString;
	fn yellow(self) -> ColoredString;

	fn bright_blue(self) -> ColoredString;
	fn bright_cyan(self) -> ColoredString;
	fn bright_black(self) -> ColoredString;

	fn bold(self) -> ColoredString;
	fn italic(self) -> ColoredString;
	fn underline(self) -> ColoredString;

	fn on_yellow(self) -> ColoredString;
}

impl<T: Into<String>> Colorize for T {
	fn red(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.foreground = Some(Color::Red);
		s
	}

	fn green(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.foreground = Some(Color::Green);
		s
	}

	fn yellow(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.foreground = Some(Color::Yellow);
		s
	}

	fn bright_blue(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.foreground = Some(Color::BrightBlue);
		s
	}

	fn bright_cyan(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.foreground = Some(Color::BrightCyan);
		s
	}

	fn bright_black(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.foreground = Some(Color::BrightBlack);
		s
	}

	fn bold(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.bold = true;
		s
	}

	fn italic(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.italic = true;
		s
	}

	fn underline(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.underline = true;
		s
	}

	fn on_yellow(self) -> ColoredString {
		let mut s = ColoredString::new(self);
		s.background = Some(Color::Yellow);
		s
	}
}

impl Colorize for ColoredString {
	fn red(mut self) -> ColoredString {
		self.foreground = Some(Color::Red);
		self
	}

	fn green(mut self) -> ColoredString {
		self.foreground = Some(Color::Green);
		self
	}

	fn yellow(mut self) -> ColoredString {
		self.foreground = Some(Color::Yellow);
		self
	}

	fn bright_blue(mut self) -> ColoredString {
		self.foreground = Some(Color::BrightBlue);
		self
	}

	fn bright_cyan(mut self) -> ColoredString {
		self.foreground = Some(Color::BrightCyan);
		self
	}

	fn bright_black(mut self) -> ColoredString {
		self.foreground = Some(Color::BrightBlack);
		self
	}

	fn bold(mut self) -> ColoredString {
		self.bold = true;
		self
	}

	fn italic(mut self) -> ColoredString {
		self.italic = true;
		self
	}

	fn underline(mut self) -> ColoredString {
		self.underline = true;
		self
	}

	fn on_yellow(mut self) -> ColoredString {
		self.background = Some(Color::Yellow);
		self
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_basic_colors() {
		let s = "Hello".red();
		assert_eq!(format!("{}", s), "\x1b[31mHello\x1b[0m");

		let s = "World".green();
		assert_eq!(format!("{}", s), "\x1b[32mWorld\x1b[0m");
	}

	#[test]
	fn test_bright_colors() {
		let s = "Test".bright_blue();
		assert_eq!(format!("{}", s), "\x1b[94mTest\x1b[0m");
	}

	#[test]
	fn test_styles() {
		let s = "Bold".bold();
		assert_eq!(format!("{}", s), "\x1b[1mBold\x1b[0m");

		let s = "Italic".italic();
		assert_eq!(format!("{}", s), "\x1b[3mItalic\x1b[0m");
	}

	#[test]
	fn test_background() {
		let s = "BG".on_yellow();
		assert_eq!(format!("{}", s), "\x1b[43mBG\x1b[0m");
	}

	#[test]
	fn test_chaining() {
		let s = "Complex".red().bold().underline();
		assert_eq!(format!("{}", s), "\x1b[1m\x1b[4m\x1b[31mComplex\x1b[0m");
	}
}
