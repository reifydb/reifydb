// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Keyword definitions.

use std::{collections::HashMap, sync::LazyLock};

macro_rules! keyword {
    ( $( $variant:ident => $string:literal ),* $(,)? ) => {
        /// RQL keywords.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Keyword {
            $( $variant ),*
        }

        impl Keyword {
            /// Get the canonical string representation.
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $( Keyword::$variant => $string ),*
                }
            }
        }

        /// Map from uppercase keyword strings to Keyword variants.
        pub static KEYWORD_MAP: LazyLock<HashMap<&'static str, Keyword>> = LazyLock::new(|| {
            let mut map = HashMap::new();
            $( map.insert($string, Keyword::$variant); )*
            map
        });
    };
}

// 65 keywords (all case-insensitive)
keyword! {
    // Query clauses
    Map        => "MAP",
    Select     => "SELECT",
    Extend     => "EXTEND",
    By         => "BY",
    From       => "FROM",
    Where      => "WHERE",
    Aggregate  => "AGGREGATE",
    Sort       => "SORT",
    Distinct   => "DISTINCT",
    Take       => "TAKE",
    Offset     => "OFFSET",
    Filter     => "FILTER",
    Scan       => "SCAN",

    // Joins
    Left       => "LEFT",
    Inner      => "INNER",
    Natural    => "NATURAL",
    Join       => "JOIN",
    On         => "ON",
    Using      => "USING",
    Merge      => "MERGE",

    // DML
    Insert     => "INSERT",
    Into       => "INTO",
    Update     => "UPDATE",
    Set        => "SET",
    Delete     => "DELETE",

    // Control flow
    Let        => "LET",
    Mut        => "MUT",
    If         => "IF",
    Else       => "ELSE",
    End        => "END",
    Loop       => "LOOP",
    Return     => "RETURN",
    For        => "FOR",

    // Functions
    Define     => "DEFINE",
    Function   => "FUNCTION",
    Call       => "CALL",
    Apply      => "APPLY",
    Cast       => "CAST",

    // DDL
    Describe   => "DESCRIBE",
    Show       => "SHOW",
    Create     => "CREATE",
    Alter      => "ALTER",
    Drop       => "DROP",
    Rename     => "RENAME",

    // Schema objects
    Namespace  => "NAMESPACE",
    Sequence   => "SEQUENCE",
    Series     => "SERIES",
    Table      => "TABLE",
    Ringbuffer => "RINGBUFFER",
    Policy     => "POLICY",
    View       => "VIEW",
    Flow       => "FLOW",
    Window     => "WINDOW",
    Dictionary => "DICTIONARY",
    Index      => "INDEX",
    Query      => "QUERY",

    // Constraints
    Unique     => "UNIQUE",
    Primary    => "PRIMARY",
    Key        => "KEY",

    // Modifiers
    Deferred      => "DEFERRED",
    Transactional => "TRANSACTIONAL",
    Asc           => "ASC",
    Desc          => "DESC",
    Auto          => "AUTO",
    Increment     => "INCREMENT",
    Value         => "VALUE",

    // Existence
    Exists     => "EXISTS",
    Replace    => "REPLACE",
    Cascade    => "CASCADE",
    Restrict   => "RESTRICT",

    // Misc
    In         => "IN",
    Between    => "BETWEEN",
    Like       => "LIKE",
    Is         => "IS",
    With       => "WITH",
    To         => "TO",
    Pause      => "PAUSE",
    Resume     => "RESUME",
    Rownum     => "ROWNUM",
}

/// Try to match an identifier string to a keyword (case-insensitive).
pub fn lookup_keyword(s: &str) -> Option<Keyword> {
	// Fast path for already uppercase
	if s.chars().all(|c| c.is_ascii_uppercase()) {
		KEYWORD_MAP.get(s).copied()
	} else {
		// Convert to uppercase for lookup
		let upper = s.to_ascii_uppercase();
		KEYWORD_MAP.get(upper.as_str()).copied()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_keyword_lookup_uppercase() {
		assert_eq!(lookup_keyword("SELECT"), Some(Keyword::Select));
		assert_eq!(lookup_keyword("MAP"), Some(Keyword::Map));
		assert_eq!(lookup_keyword("SCAN"), Some(Keyword::Scan));
	}

	#[test]
	fn test_keyword_lookup_lowercase() {
		assert_eq!(lookup_keyword("select"), Some(Keyword::Select));
		assert_eq!(lookup_keyword("map"), Some(Keyword::Map));
		assert_eq!(lookup_keyword("scan"), Some(Keyword::Scan));
	}

	#[test]
	fn test_keyword_lookup_mixed_case() {
		assert_eq!(lookup_keyword("Select"), Some(Keyword::Select));
		assert_eq!(lookup_keyword("MaP"), Some(Keyword::Map));
		assert_eq!(lookup_keyword("ScAn"), Some(Keyword::Scan));
	}

	#[test]
	fn test_keyword_lookup_not_found() {
		assert_eq!(lookup_keyword("foobar"), None);
		assert_eq!(lookup_keyword("SELECTX"), None);
	}

	#[test]
	fn test_keyword_as_str() {
		assert_eq!(Keyword::Select.as_str(), "SELECT");
		assert_eq!(Keyword::Map.as_str(), "MAP");
		assert_eq!(Keyword::Scan.as_str(), "SCAN");
	}
}
