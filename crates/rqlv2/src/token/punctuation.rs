// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Punctuation definitions.

macro_rules! punctuation {
    ( $( $variant:ident => $symbol:literal ),* $(,)? ) => {
        /// RQL punctuation (delimiters and separators).
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Punctuation {
            $( $variant ),*
        }

        impl Punctuation {
            /// Get the string representation.
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $( Punctuation::$variant => $symbol ),*
                }
            }
        }
    };
}

punctuation! {
    // Brackets/Parens
    OpenParen    => "(",
    CloseParen   => ")",
    OpenBracket  => "[",
    CloseBracket => "]",
    OpenCurly    => "{",
    CloseCurly   => "}",

    // Separators
    Comma     => ",",
    Semicolon => ";",
    Newline   => "\n",
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_punctuation_as_str() {
		assert_eq!(Punctuation::OpenParen.as_str(), "(");
		assert_eq!(Punctuation::CloseParen.as_str(), ")");
		assert_eq!(Punctuation::Comma.as_str(), ",");
		assert_eq!(Punctuation::Semicolon.as_str(), ";");
	}
}
