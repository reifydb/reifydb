// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Trait for calculating display width of strings
pub trait UnicodeWidthStr {
	fn width(&self) -> usize;
}

impl UnicodeWidthStr for str {
	fn width(&self) -> usize {
		self.chars().map(char_width).sum()
	}
}

impl UnicodeWidthStr for &str {
	fn width(&self) -> usize {
		self.chars().map(char_width).sum()
	}
}

fn char_width(ch: char) -> usize {
	match ch {
		'\x00'..='\x1F' | '\x7F'..='\u{9F}' => 0,

		'\u{1100}'..='\u{115F}'
		| '\u{2E80}'..='\u{2EFF}'
		| '\u{2F00}'..='\u{2FDF}'
		| '\u{3000}'..='\u{303F}'
		| '\u{3040}'..='\u{309F}'
		| '\u{30A0}'..='\u{30FF}'
		| '\u{3100}'..='\u{312F}'
		| '\u{3130}'..='\u{318F}'
		| '\u{31A0}'..='\u{31BF}'
		| '\u{31F0}'..='\u{31FF}'
		| '\u{3200}'..='\u{32FF}'
		| '\u{3300}'..='\u{33FF}'
		| '\u{3400}'..='\u{4DBF}'
		| '\u{4E00}'..='\u{9FFF}'
		| '\u{A000}'..='\u{A48F}'
		| '\u{A490}'..='\u{A4CF}'
		| '\u{AC00}'..='\u{D7AF}'
		| '\u{F900}'..='\u{FAFF}'
		| '\u{FE30}'..='\u{FE4F}'
		| '\u{FF00}'..='\u{FF60}'
		| '\u{FFE0}'..='\u{FFE6}'
		| '\u{20000}'..='\u{2FFFD}' => 2,

		'\u{1F300}'..='\u{1F6FF}'
		| '\u{1F700}'..='\u{1F77F}'
		| '\u{1F780}'..='\u{1F7FF}'
		| '\u{1F800}'..='\u{1F8FF}'
		| '\u{1F900}'..='\u{1F9FF}'
		| '\u{1FA00}'..='\u{1FA6F}'
		| '\u{1FA70}'..='\u{1FAFF}' => 2,

		'\u{200B}'..='\u{200F}' | '\u{2028}'..='\u{202E}' | '\u{2060}'..='\u{206F}' => 0,

		'\u{0300}'..='\u{036F}'
		| '\u{1AB0}'..='\u{1AFF}'
		| '\u{1DC0}'..='\u{1DFF}'
		| '\u{FE20}'..='\u{FE2F}' => 0,

		'\u{FE00}'..='\u{FE0F}' => 0,
		'\u{E0100}'..='\u{E01EF}' => 0,

		'\u{2600}'..='\u{27BF}' => 2,

		_ => 1,
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_ascii() {
		assert_eq!("Hello".width(), 5);
		assert_eq!("Hello, World!".width(), 13);
		assert_eq!("".width(), 0);
	}

	#[test]
	fn test_cjk() {
		assert_eq!("你好".width(), 4); // Two Chinese characters
		assert_eq!("こんにちは".width(), 10); // Five Japanese characters
		assert_eq!("안녕하세요".width(), 10); // Five Korean characters
	}

	#[test]
	fn test_mixed() {
		assert_eq!("Hello 世界".width(), 10); // 6 for "Hello " + 4 for two CJK chars
	}

	#[test]
	fn test_control_chars() {
		assert_eq!("\x00\x01\x02".width(), 0);
		assert_eq!("Hello\nWorld".width(), 10); // newline has 0 width
		assert_eq!("Hello\tWorld".width(), 10); // tab has 0 width
	}

	#[test]
	fn test_combining_marks() {
		// e with combining acute accent
		assert_eq!("e\u{0301}".width(), 1);
		// a with combining tilde
		assert_eq!("a\u{0303}".width(), 1);
	}

	#[test]
	fn test_emoji() {
		assert_eq!("🚀".width(), 2); // Rocket emoji
		assert_eq!("😀".width(), 2); // Smiley face
		assert_eq!("🎉".width(), 2); // Party popper
		assert_eq!("Unicode: 🚀 ñ é ü".width(), 17); // "Unicode: " (9) + 🚀 (2) + " ñ é ü" (6) = 17
	}
}
