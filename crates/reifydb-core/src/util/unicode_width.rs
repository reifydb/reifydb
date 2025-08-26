// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Simple unicode width calculation implementation

/// Trait for calculating display width of strings
pub trait UnicodeWidthStr {
	/// Returns the display width of the string
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

/// Calculate the display width of a single character
fn char_width(ch: char) -> usize {
	match ch {
        // Control characters have no width (includes \n, \r, \t)
        '\x00'..='\x1F' | '\x7F'..='\u{9F}' => 0,

        // Most CJK characters are double-width
        '\u{1100}'..='\u{115F}' |  // Hangul Jamo
        '\u{2E80}'..='\u{2EFF}' |  // CJK Radicals Supplement
        '\u{2F00}'..='\u{2FDF}' |  // Kangxi Radicals
        '\u{3000}'..='\u{303F}' |  // CJK Symbols and Punctuation
        '\u{3040}'..='\u{309F}' |  // Hiragana
        '\u{30A0}'..='\u{30FF}' |  // Katakana
        '\u{3100}'..='\u{312F}' |  // Bopomofo
        '\u{3130}'..='\u{318F}' |  // Hangul Compatibility Jamo
        '\u{31A0}'..='\u{31BF}' |  // Bopomofo Extended
        '\u{31F0}'..='\u{31FF}' |  // Katakana Phonetic Extensions
        '\u{3200}'..='\u{32FF}' |  // Enclosed CJK Letters and Months
        '\u{3300}'..='\u{33FF}' |  // CJK Compatibility
        '\u{3400}'..='\u{4DBF}' |  // CJK Unified Ideographs Extension A
        '\u{4E00}'..='\u{9FFF}' |  // CJK Unified Ideographs
        '\u{A000}'..='\u{A48F}' |  // Yi Syllables
        '\u{A490}'..='\u{A4CF}' |  // Yi Radicals
        '\u{AC00}'..='\u{D7AF}' |  // Hangul Syllables
        '\u{F900}'..='\u{FAFF}' |  // CJK Compatibility Ideographs
        '\u{FE30}'..='\u{FE4F}' |  // CJK Compatibility Forms
        '\u{FF00}'..='\u{FF60}' |  // Fullwidth Forms (part)
        '\u{FFE0}'..='\u{FFE6}' |  // Fullwidth Forms (part)
        '\u{20000}'..='\u{2FFFD}' => 2,

        // Emoji and symbols (generally double-width)
        '\u{1F300}'..='\u{1F6FF}' |  // Emoji & Pictographs (includes Regional Indicators U+1F1E6-1F1FF)
        '\u{1F700}'..='\u{1F77F}' |  // Alchemical Symbols
        '\u{1F780}'..='\u{1F7FF}' |  // Geometric Shapes Extended
        '\u{1F800}'..='\u{1F8FF}' |  // Supplemental Arrows-C
        '\u{1F900}'..='\u{1F9FF}' |  // Supplemental Symbols and Pictographs
        '\u{1FA00}'..='\u{1FA6F}' |  // Chess Symbols
        '\u{1FA70}'..='\u{1FAFF}' => 2, // Symbols and Pictographs Extended-A

        // Zero-width characters
        '\u{200B}'..='\u{200F}' |  // Zero width space, joiners, etc.
        '\u{2028}'..='\u{202E}' |  // Line/paragraph separators, directional formatting
        '\u{2060}'..='\u{206F}' => 0,

        // Combining marks (zero width)
        '\u{0300}'..='\u{036F}' |  // Combining Diacritical Marks
        '\u{1AB0}'..='\u{1AFF}' |  // Combining Diacritical Marks Extended
        '\u{1DC0}'..='\u{1DFF}' |  // Combining Diacritical Marks for Symbols
        '\u{FE20}'..='\u{FE2F}' => 0,

        // Variation selectors (zero width)
        '\u{FE00}'..='\u{FE0F}' => 0,
        '\u{E0100}'..='\u{E01EF}' => 0,

        // Some specific double-width symbols
        '\u{2600}'..='\u{27BF}' => 2, // Miscellaneous Symbols, Dingbats
        // Default: single width
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
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
