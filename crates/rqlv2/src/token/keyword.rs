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

        /// Map from lowercase keyword strings to Keyword variants.
        pub static KEYWORD_MAP: LazyLock<HashMap<String, Keyword>> = LazyLock::new(|| {
            let mut map = HashMap::new();
            $( map.insert($string.to_ascii_lowercase(), Keyword::$variant); )*
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
    Then       => "THEN",
    Else       => "ELSE",
    End        => "END",
    Loop       => "LOOP",
    Return     => "RETURN",
    For        => "FOR",
    Break      => "BREAK",
    Continue   => "CONTINUE",

    // Boolean literals (treated as keywords)
    True       => "TRUE",
    False      => "FALSE",
    Undefined  => "UNDEFINED",

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
	// Fast path for already lowercase
	if s.chars().all(|c| c.is_ascii_lowercase()) {
		KEYWORD_MAP.get(s).copied()
	} else {
		// Convert to lowercase for lookup
		let lower = s.to_ascii_lowercase();
		KEYWORD_MAP.get(lower.as_str()).copied()
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use super::*;
	use crate::token::{Lexer, TokenKind};

	fn tokenize(source: &str) -> Result<Vec<crate::token::Token>, crate::token::LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		Ok(result.tokens.into_iter().collect())
	}

	fn tokenize_with_text(source: &str) -> Result<(Vec<crate::token::Token>, String), crate::token::LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		let source_copy = result.source.to_string();
		Ok((result.tokens.into_iter().collect(), source_copy))
	}

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

	#[test]
	fn test_desc() {
		let tokens = tokenize("desc").unwrap();
		assert!(tokens.len() >= 1);
		assert!(matches!(tokens[0].kind, TokenKind::Keyword(Keyword::Desc)));
	}

	fn check_keyword(keyword: Keyword, repr: &str) {
		// Test both uppercase and lowercase
		for mode in [false, true] {
			let input_str = if mode {
				format!("{repr} rest")
			} else {
				format!("{} rest", repr.to_lowercase())
			};

			let (tokens, source) = tokenize_with_text(&input_str).unwrap();
			assert!(tokens.len() >= 2, "Expected at least 2 tokens for '{}'", input_str);
			assert_eq!(tokens[0].kind, TokenKind::Keyword(keyword), "Type mismatch for keyword: {}", repr);
			assert_eq!(tokens[0].text(&source).to_lowercase(), repr.to_lowercase());
		}
	}

	macro_rules! generate_keyword_tests {
		($($name:ident => ($variant:ident, $repr:literal)),* $(,)?) => {
			$(
				#[test]
				fn $name() {
					check_keyword(Keyword::$variant, $repr);
				}
			)*
		};
	}

	generate_keyword_tests! {
		test_keyword_map => (Map, "MAP"),
		test_keyword_select => (Select, "SELECT"),
		test_keyword_extend => (Extend, "EXTEND"),
		test_keyword_by => (By, "BY"),
		test_keyword_from => (From, "FROM"),
		test_keyword_where => (Where, "WHERE"),
		test_keyword_aggregate => (Aggregate, "AGGREGATE"),
		test_keyword_sort => (Sort, "SORT"),
		test_keyword_distinct => (Distinct, "DISTINCT"),
		test_keyword_take => (Take, "TAKE"),
		test_keyword_offset => (Offset, "OFFSET"),
		test_keyword_filter => (Filter, "FILTER"),
		test_keyword_scan => (Scan, "SCAN"),
		test_keyword_left => (Left, "LEFT"),
		test_keyword_inner => (Inner, "INNER"),
		test_keyword_natural => (Natural, "NATURAL"),
		test_keyword_join => (Join, "JOIN"),
		test_keyword_on => (On, "ON"),
		test_keyword_using => (Using, "USING"),
		test_keyword_merge => (Merge, "MERGE"),
		test_keyword_insert => (Insert, "INSERT"),
		test_keyword_into => (Into, "INTO"),
		test_keyword_update => (Update, "UPDATE"),
		test_keyword_set => (Set, "SET"),
		test_keyword_delete => (Delete, "DELETE"),
		test_keyword_let => (Let, "LET"),
		test_keyword_mut => (Mut, "MUT"),
		test_keyword_if => (If, "IF"),
		test_keyword_then => (Then, "THEN"),
		test_keyword_else => (Else, "ELSE"),
		test_keyword_end => (End, "END"),
		test_keyword_loop => (Loop, "LOOP"),
		test_keyword_return => (Return, "RETURN"),
		test_keyword_for => (For, "FOR"),
		test_keyword_break => (Break, "BREAK"),
		test_keyword_continue => (Continue, "CONTINUE"),
		test_keyword_define => (Define, "DEFINE"),
		test_keyword_function => (Function, "FUNCTION"),
		test_keyword_call => (Call, "CALL"),
		test_keyword_apply => (Apply, "APPLY"),
		test_keyword_cast => (Cast, "CAST"),
		test_keyword_describe => (Describe, "DESCRIBE"),
		test_keyword_show => (Show, "SHOW"),
		test_keyword_create => (Create, "CREATE"),
		test_keyword_alter => (Alter, "ALTER"),
		test_keyword_drop => (Drop, "DROP"),
		test_keyword_rename => (Rename, "RENAME"),
		test_keyword_namespace => (Namespace, "NAMESPACE"),
		test_keyword_sequence => (Sequence, "SEQUENCE"),
		test_keyword_series => (Series, "SERIES"),
		test_keyword_table => (Table, "TABLE"),
		test_keyword_ringbuffer => (Ringbuffer, "RINGBUFFER"),
		test_keyword_policy => (Policy, "POLICY"),
		test_keyword_view => (View, "VIEW"),
		test_keyword_flow => (Flow, "FLOW"),
		test_keyword_window => (Window, "WINDOW"),
		test_keyword_dictionary => (Dictionary, "DICTIONARY"),
		test_keyword_index => (Index, "INDEX"),
		test_keyword_query => (Query, "QUERY"),
		test_keyword_unique => (Unique, "UNIQUE"),
		test_keyword_primary => (Primary, "PRIMARY"),
		test_keyword_key => (Key, "KEY"),
		test_keyword_deferred => (Deferred, "DEFERRED"),
		test_keyword_transactional => (Transactional, "TRANSACTIONAL"),
		test_keyword_asc => (Asc, "ASC"),
		test_keyword_desc2 => (Desc, "DESC"),
		test_keyword_auto => (Auto, "AUTO"),
		test_keyword_increment => (Increment, "INCREMENT"),
		test_keyword_value => (Value, "VALUE"),
		test_keyword_exists => (Exists, "EXISTS"),
		test_keyword_replace => (Replace, "REPLACE"),
		test_keyword_cascade => (Cascade, "CASCADE"),
		test_keyword_restrict => (Restrict, "RESTRICT"),
		test_keyword_in => (In, "IN"),
		test_keyword_between => (Between, "BETWEEN"),
		test_keyword_like => (Like, "LIKE"),
		test_keyword_is => (Is, "IS"),
		test_keyword_with => (With, "WITH"),
		test_keyword_to => (To, "TO"),
		test_keyword_pause => (Pause, "PAUSE"),
		test_keyword_resume => (Resume, "RESUME"),
		test_keyword_rownum => (Rownum, "ROWNUM"),
	}

	fn check_no_keyword(repr: &str) {
		// Test that keywords with additional characters are NOT parsed as keywords
		let test_cases = vec![
			format!("{repr}_something_else"), // e.g., "map_something_else"
			format!("{repr}SomethingElse"),   // e.g., "mapSomethingElse"
			format!("{repr}123"),             // e.g., "map123"
			format!("_{repr}"),               // e.g., "_map"
		];

		for input_str in test_cases {
			let input = format!("{input_str} rest");
			let tokens = tokenize(&input).unwrap();
			assert!(tokens.len() >= 1);
			// The first token should be an identifier, not a keyword
			assert_eq!(
				tokens[0].kind,
				TokenKind::Identifier,
				"Input '{}' should produce an identifier, not a keyword",
				input_str
			);
		}

		// Also test that the bare lowercase word IS parsed as a keyword
		let input = format!("{repr} rest");
		let tokens = tokenize(&input).unwrap();
		assert!(tokens.len() >= 2);
		assert!(
			matches!(tokens[0].kind, TokenKind::Keyword(_)),
			"Input '{}' should be parsed as a keyword",
			repr
		);
	}

	macro_rules! generate_not_keyword_tests {
		($($name:ident => $repr:literal),* $(,)?) => {
			$(
				#[test]
				fn $name() {
					check_no_keyword($repr);
				}
			)*
		};
	}

	generate_not_keyword_tests! {
		test_not_keyword_map => "map",
		test_not_keyword_select => "select",
		test_not_keyword_extend => "extend",
		test_not_keyword_by => "by",
		test_not_keyword_from => "from",
		test_not_keyword_where => "where",
		test_not_keyword_aggregate => "aggregate",
		test_not_keyword_sort => "sort",
		test_not_keyword_distinct => "distinct",
		test_not_keyword_take => "take",
		test_not_keyword_offset => "offset",
		test_not_keyword_filter => "filter",
		test_not_keyword_scan => "scan",
		test_not_keyword_left => "left",
		test_not_keyword_inner => "inner",
		test_not_keyword_natural => "natural",
		test_not_keyword_join => "join",
		test_not_keyword_on => "on",
		test_not_keyword_using => "using",
		test_not_keyword_merge => "merge",
		test_not_keyword_insert => "insert",
		test_not_keyword_into => "into",
		test_not_keyword_update => "update",
		test_not_keyword_set => "set",
		test_not_keyword_delete => "delete",
		test_not_keyword_let => "let",
		test_not_keyword_mut => "mut",
		test_not_keyword_if => "if",
		test_not_keyword_then => "then",
		test_not_keyword_else => "else",
		test_not_keyword_end => "end",
		test_not_keyword_loop => "loop",
		test_not_keyword_return => "return",
		test_not_keyword_for => "for",
		test_not_keyword_break => "break",
		test_not_keyword_continue => "continue",
		test_not_keyword_define => "define",
		test_not_keyword_function => "function",
		test_not_keyword_call => "call",
		test_not_keyword_apply => "apply",
		test_not_keyword_cast => "cast",
		test_not_keyword_describe => "describe",
		test_not_keyword_show => "show",
		test_not_keyword_create => "create",
		test_not_keyword_alter => "alter",
		test_not_keyword_drop => "drop",
		test_not_keyword_rename => "rename",
		test_not_keyword_namespace => "namespace",
		test_not_keyword_sequence => "sequence",
		test_not_keyword_series => "series",
		test_not_keyword_table => "table",
		test_not_keyword_ringbuffer => "ringbuffer",
		test_not_keyword_policy => "policy",
		test_not_keyword_view => "view",
		test_not_keyword_flow => "flow",
		test_not_keyword_window => "window",
		test_not_keyword_dictionary => "dictionary",
		test_not_keyword_index => "index",
		test_not_keyword_query => "query",
		test_not_keyword_unique => "unique",
		test_not_keyword_primary => "primary",
		test_not_keyword_key => "key",
		test_not_keyword_deferred => "deferred",
		test_not_keyword_transactional => "transactional",
		test_not_keyword_asc => "asc",
		test_not_keyword_desc => "desc",
		test_not_keyword_auto => "auto",
		test_not_keyword_increment => "increment",
		test_not_keyword_value => "value",
		test_not_keyword_exists => "exists",
		test_not_keyword_replace => "replace",
		test_not_keyword_cascade => "cascade",
		test_not_keyword_restrict => "restrict",
		test_not_keyword_in => "in",
		test_not_keyword_between => "between",
		test_not_keyword_like => "like",
		test_not_keyword_is => "is",
		test_not_keyword_with => "with",
		test_not_keyword_to => "to",
		test_not_keyword_pause => "pause",
		test_not_keyword_resume => "resume",
		test_not_keyword_rownum => "rownum",
	}
}
