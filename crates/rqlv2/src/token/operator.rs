// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operator definitions.

use std::{collections::HashMap, sync::LazyLock};

macro_rules! operator {
    ( $( $variant:ident => $symbol:literal ),* $(,)? ) => {
        /// RQL operators.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Operator {
            $( $variant ),*
        }

        impl Operator {
            /// Get the string representation.
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $( Operator::$variant => $symbol ),*
                }
            }
        }
    };
}

operator! {
    // Comparison
    Equal           => "=",
    DoubleEqual     => "==",
    BangEqual       => "!=",
    LeftAngle       => "<",
    LeftAngleEqual  => "<=",
    RightAngle      => ">",
    RightAngleEqual => ">=",

    // Arithmetic
    Plus      => "+",
    Minus     => "-",
    Asterisk  => "*",
    Slash     => "/",
    Percent   => "%",
    Caret     => "^",

    // Logical (symbolic)
    Bang              => "!",
    Ampersand         => "&",
    DoubleAmpersand   => "&&",
    Pipe              => "|",
    DoublePipe        => "||",

    // Logical (word) - case insensitive
    And  => "AND",
    Or   => "OR",
    Not  => "NOT",
    Xor  => "XOR",

    // As keyword (used as operator)
    As   => "AS",

    // Access/navigation
    Dot           => ".",
    DoubleDot     => "..",
    Colon         => ":",
    DoubleColon   => "::",
    Arrow         => "->",
    QuestionMark  => "?",

    // Bit shift
    DoubleLeftAngle  => "<<",
    DoubleRightAngle => ">>",
}

/// Map from uppercase word operator strings to Operator variants.
pub static WORD_OPERATORS: LazyLock<HashMap<&'static str, Operator>> = LazyLock::new(|| {
	let mut map = HashMap::new();
	map.insert("AND", Operator::And);
	map.insert("OR", Operator::Or);
	map.insert("NOT", Operator::Not);
	map.insert("XOR", Operator::Xor);
	map.insert("AS", Operator::As);
	map
});

/// Try to match an identifier string to a word operator (case-insensitive).
pub fn lookup_word_operator(s: &str) -> Option<Operator> {
	let upper = s.to_ascii_uppercase();
	WORD_OPERATORS.get(upper.as_str()).copied()
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_word_operator_lookup() {
		assert_eq!(lookup_word_operator("AND"), Some(Operator::And));
		assert_eq!(lookup_word_operator("and"), Some(Operator::And));
		assert_eq!(lookup_word_operator("And"), Some(Operator::And));
		assert_eq!(lookup_word_operator("OR"), Some(Operator::Or));
		assert_eq!(lookup_word_operator("NOT"), Some(Operator::Not));
		assert_eq!(lookup_word_operator("XOR"), Some(Operator::Xor));
		assert_eq!(lookup_word_operator("AS"), Some(Operator::As));
	}

	#[test]
	fn test_word_operator_not_found() {
		assert_eq!(lookup_word_operator("SELECT"), None);
		assert_eq!(lookup_word_operator("foobar"), None);
	}

	#[test]
	fn test_operator_as_str() {
		assert_eq!(Operator::Plus.as_str(), "+");
		assert_eq!(Operator::DoubleEqual.as_str(), "==");
		assert_eq!(Operator::And.as_str(), "AND");
	}
}
