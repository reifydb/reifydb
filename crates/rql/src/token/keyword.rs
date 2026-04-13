// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::LazyLock};

use reifydb_type::{
	error::{AstErrorKind, Error, TypeError},
	fragment::Fragment,
};

use super::{
	cursor::Cursor,
	identifier::is_identifier_char,
	token::{Token, TokenKind},
};
use crate::Result;

macro_rules! keyword {
    (
        $( $variant:ident => $string:literal ),* $(,)?
    ) => {

        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub enum Keyword {
            $( $variant ),*
        }

        impl Keyword {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $( Keyword::$variant => $string ),*
                }
            }
        }

        impl TryFrom<&str> for Keyword {
            type Error = Error;

            fn try_from(value: &str) -> Result<Self> {
                debug_assert!(value.chars().all(|c| c.is_uppercase()), "keyword must be uppercase");
                match value {
                    $( $string => Ok(Keyword::$variant) ),*,
                    _ => {
                        let message = "not a keyword".to_string();
                        Err(Error::from(TypeError::Ast {
                            kind: AstErrorKind::TokenizeError { message: message.clone() },
                            message,
                            fragment: Fragment::None,
                        }))
                    }
                }
            }
        }
    };
}

keyword! {
Map     => "MAP",
Extend  => "EXTEND",
By         => "BY",
From       => "FROM",
Aggregate  => "AGGREGATE",
Sort      => "SORT",
Distinct   => "DISTINCT",
Take      => "TAKE",

Left       => "LEFT",
Inner      => "INNER",
Natural    => "NATURAL",
Join       => "JOIN",
On         => "ON",
Using      => "USING",

Insert     => "INSERT",
Update     => "UPDATE",
Set        => "SET",
Delete     => "DELETE",

Let        => "LET",
If         => "IF",
Else       => "ELSE",
Loop       => "LOOP",
While      => "WHILE",
Break      => "BREAK",
Continue   => "CONTINUE",
Return     => "RETURN",

Udf        => "UDF",
Call       => "CALL",
Apply      => "APPLY",
Cast       => "CAST",

Describe   => "DESCRIBE",
Create     => "CREATE",
Alter      => "ALTER",
Drop       => "DROP",
Filter     => "FILTER",
Gate       => "GATE",
Window     => "WINDOW",


In         => "IN",
Between    => "BETWEEN",
Is         => "IS",
With       => "WITH",

Namespace => "NAMESPACE",
Sequence => "SEQUENCE",
Series  => "SERIES",
Subscription => "SUBSCRIPTION",
Table  => "TABLE",
Ringbuffer => "RINGBUFFER",
Column => "COLUMN",
Policy => "POLICY",
Property => "PROPERTY",
View => "VIEW",
Deferred => "DEFERRED",
Transactional => "TRANSACTIONAL",

Index => "INDEX",
Unique => "UNIQUE",
Primary => "PRIMARY",
Key => "KEY",
Asc => "ASC",
Desc => "DESC",
Value => "VALUE",

Exists => "EXISTS",
Replace => "REPLACE",
Cascade => "CASCADE",
Restrict => "RESTRICT",
To => "TO",
Rename => "RENAME",
Rownum => "ROWNUM",
Dictionary => "DICTIONARY",
For => "FOR",
Output => "OUTPUT",
Append => "APPEND",
Assert => "ASSERT",
Patch => "PATCH",
Enum => "ENUM",
Match => "MATCH",
Procedure => "PROCEDURE",
Event => "EVENT",
Handler => "HANDLER",
Dispatch => "DISPATCH",
Tag => "TAG",
Test => "TEST",
Tests => "TESTS",
Run => "RUN",
User => "USER",
Role => "ROLE",
Grant => "GRANT",
Revoke => "REVOKE",
Require => "REQUIRE",
Enable => "ENABLE",
Disable => "DISABLE",
Function => "FUNCTION",
Session => "SESSION",
Feature => "FEATURE",
Add => "ADD",
Migration => "MIGRATION",
Migrate => "MIGRATE",
Rollback => "ROLLBACK",
Authentication => "AUTHENTICATION",
Contains => "CONTAINS",
Remote => "REMOTE",
Error => "ERROR",
Returning => "RETURNING",
Source => "SOURCE",
Sink => "SINK",
}

static KEYWORD_MAP: LazyLock<HashMap<&'static str, Keyword>> = LazyLock::new(|| {
	let mut map = HashMap::new();
	map.insert("MAP", Keyword::Map);
	map.insert("APPLY", Keyword::Apply);
	map.insert("EXTEND", Keyword::Extend);
	map.insert("BY", Keyword::By);
	map.insert("FROM", Keyword::From);
	map.insert("AGGREGATE", Keyword::Aggregate);
	map.insert("SORT", Keyword::Sort);
	map.insert("DISTINCT", Keyword::Distinct);
	map.insert("TAKE", Keyword::Take);
	map.insert("LEFT", Keyword::Left);
	map.insert("INNER", Keyword::Inner);
	map.insert("NATURAL", Keyword::Natural);
	map.insert("JOIN", Keyword::Join);
	map.insert("ON", Keyword::On);
	map.insert("USING", Keyword::Using);
	map.insert("INSERT", Keyword::Insert);
	map.insert("UPDATE", Keyword::Update);
	map.insert("SET", Keyword::Set);
	map.insert("DELETE", Keyword::Delete);
	map.insert("LET", Keyword::Let);
	map.insert("IF", Keyword::If);
	map.insert("ELSE", Keyword::Else);
	map.insert("LOOP", Keyword::Loop);
	map.insert("WHILE", Keyword::While);
	map.insert("BREAK", Keyword::Break);
	map.insert("CONTINUE", Keyword::Continue);
	map.insert("RETURN", Keyword::Return);
	map.insert("UDF", Keyword::Udf);
	map.insert("CALL", Keyword::Call);
	map.insert("CAST", Keyword::Cast);
	map.insert("DESCRIBE", Keyword::Describe);
	map.insert("CREATE", Keyword::Create);
	map.insert("ALTER", Keyword::Alter);
	map.insert("DROP", Keyword::Drop);
	map.insert("FILTER", Keyword::Filter);
	map.insert("GATE", Keyword::Gate);
	map.insert("WINDOW", Keyword::Window);
	map.insert("IN", Keyword::In);
	map.insert("BETWEEN", Keyword::Between);
	map.insert("IS", Keyword::Is);
	map.insert("WITH", Keyword::With);
	map.insert("NAMESPACE", Keyword::Namespace);
	map.insert("SEQUENCE", Keyword::Sequence);
	map.insert("SERIES", Keyword::Series);
	map.insert("SUBSCRIPTION", Keyword::Subscription);
	map.insert("TABLE", Keyword::Table);
	map.insert("RINGBUFFER", Keyword::Ringbuffer);
	map.insert("COLUMN", Keyword::Column);
	map.insert("POLICY", Keyword::Policy);
	map.insert("PROPERTY", Keyword::Property);
	map.insert("VIEW", Keyword::View);
	map.insert("DEFERRED", Keyword::Deferred);
	map.insert("TRANSACTIONAL", Keyword::Transactional);
	map.insert("INDEX", Keyword::Index);
	map.insert("UNIQUE", Keyword::Unique);
	map.insert("PRIMARY", Keyword::Primary);
	map.insert("KEY", Keyword::Key);
	map.insert("ASC", Keyword::Asc);
	map.insert("DESC", Keyword::Desc);
	map.insert("VALUE", Keyword::Value);
	map.insert("EXISTS", Keyword::Exists);
	map.insert("REPLACE", Keyword::Replace);
	map.insert("CASCADE", Keyword::Cascade);
	map.insert("RESTRICT", Keyword::Restrict);
	map.insert("TO", Keyword::To);
	map.insert("RENAME", Keyword::Rename);
	map.insert("ROWNUM", Keyword::Rownum);
	map.insert("DICTIONARY", Keyword::Dictionary);
	map.insert("FOR", Keyword::For);
	map.insert("OUTPUT", Keyword::Output);
	map.insert("APPEND", Keyword::Append);
	map.insert("ASSERT", Keyword::Assert);
	map.insert("PATCH", Keyword::Patch);
	map.insert("ENUM", Keyword::Enum);
	map.insert("MATCH", Keyword::Match);
	map.insert("PROCEDURE", Keyword::Procedure);
	map.insert("EVENT", Keyword::Event);
	map.insert("HANDLER", Keyword::Handler);
	map.insert("DISPATCH", Keyword::Dispatch);
	map.insert("TAG", Keyword::Tag);
	map.insert("TEST", Keyword::Test);
	map.insert("TESTS", Keyword::Tests);
	map.insert("RUN", Keyword::Run);
	map.insert("USER", Keyword::User);
	map.insert("ROLE", Keyword::Role);
	map.insert("GRANT", Keyword::Grant);
	map.insert("REVOKE", Keyword::Revoke);
	map.insert("REQUIRE", Keyword::Require);
	map.insert("ENABLE", Keyword::Enable);
	map.insert("DISABLE", Keyword::Disable);
	map.insert("FUNCTION", Keyword::Function);
	map.insert("SESSION", Keyword::Session);
	map.insert("FEATURE", Keyword::Feature);
	map.insert("ADD", Keyword::Add);
	map.insert("MIGRATION", Keyword::Migration);
	map.insert("MIGRATE", Keyword::Migrate);
	map.insert("ROLLBACK", Keyword::Rollback);
	map.insert("AUTHENTICATION", Keyword::Authentication);
	map.insert("CONTAINS", Keyword::Contains);
	map.insert("REMOTE", Keyword::Remote);
	map.insert("ERROR", Keyword::Error);
	map.insert("RETURNING", Keyword::Returning);
	map.insert("SOURCE", Keyword::Source);
	map.insert("SINK", Keyword::Sink);
	map
});

/// Scan for a keyword token
pub fn scan_keyword<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	// Keywords must start with a letter, so check that first
	let first_char = cursor.peek()?;
	if !first_char.is_ascii_alphabetic() {
		return None;
	}

	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	// Consume identifier characters to get the potential keyword
	let remaining = cursor.remaining_input();
	let word_len = remaining.chars().take_while(|&c| is_identifier_char(c)).map(|c| c.len_utf8()).sum::<usize>();

	let word = &remaining[..word_len];

	// Check if it's a keyword (case-insensitive)
	let uppercase_word;
	let lookup_word = if word.chars().all(|c| c.is_uppercase()) {
		word
	} else {
		uppercase_word = word.to_uppercase();
		&uppercase_word
	};

	if let Some(&keyword) = KEYWORD_MAP.get(lookup_word) {
		// Check that the next character is not an identifier
		// continuation
		let next_char = cursor.peek_ahead(word.chars().count());
		if next_char.is_none_or(|ch| !is_identifier_char(ch) && ch != '.' && ch != ':') {
			// Consume the keyword
			for _ in 0..word.chars().count() {
				cursor.consume();
			}
			return Some(Token {
				kind: TokenKind::Keyword(keyword),
				fragment: cursor.make_fragment(start_pos, start_line, start_column),
			});
		}
	}

	None
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{bump::Bump, token::tokenize};

	#[test]
	fn test_desc() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "desc").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Desc));
	}

	fn check_keyword(keyword: Keyword, repr: &str) {
		let bump = Bump::new();
		for mode in [false, true] {
			let input_str = if mode {
				format!("{repr} rest")
			} else {
				format!("{} rest", repr.to_lowercase())
			};

			let tokens = tokenize(&bump, &input_str).unwrap();
			assert!(tokens.len() >= 2);
			assert_eq!(TokenKind::Keyword(keyword), tokens[0].kind, "type mismatch for keyword: {}", repr);
			assert_eq!(tokens[0].fragment.text().to_lowercase(), repr.to_lowercase());
			assert_eq!(tokens[0].fragment.column().0, 1);
			assert_eq!(tokens[0].fragment.line().0, 1);
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
	test_keyword_apply => (Apply, "APPLY"),
	test_keyword_by => (By, "BY"),
	test_keyword_from => (From, "FROM"),
	test_keyword_aggregate => (Aggregate, "AGGREGATE"),
	test_keyword_sort => (Sort, "SORT"),
	test_keyword_take => (Take, "TAKE"),
	test_keyword_insert => (Insert, "INSERT"),
	test_keyword_update => (Update, "UPDATE"),
	test_keyword_set => (Set, "SET"),
	test_keyword_delete => (Delete, "DELETE"),
	test_keyword_left => (Left, "LEFT"),
	test_keyword_inner => (Inner, "INNER"),
	test_keyword_natural => (Natural, "NATURAL"),
	test_keyword_join => (Join, "JOIN"),
	test_keyword_on => (On, "ON"),
	test_keyword_using => (Using, "USING"),
	test_keyword_let => (Let, "LET"),
	test_keyword_if => (If, "IF"),
	test_keyword_else => (Else, "ELSE"),
	test_keyword_loop => (Loop, "LOOP"),
	test_keyword_while => (While, "WHILE"),
	test_keyword_break => (Break, "BREAK"),
	test_keyword_continue => (Continue, "CONTINUE"),
	test_keyword_return => (Return, "RETURN"),
	test_keyword_udf => (Udf, "UDF"),
	test_keyword_call => (Call, "CALL"),
	test_keyword_describe => (Describe, "DESCRIBE"),
	test_keyword_create => (Create, "CREATE"),
	test_keyword_drop => (Drop, "DROP"),
	test_keyword_in => (In, "IN"),
	test_keyword_between => (Between, "BETWEEN"),
	test_keyword_is => (Is, "IS"),
	test_keyword_with => (With, "WITH"),
	test_keyword_is_in => (Filter, "FILTER"),
	test_keyword_gate => (Gate, "GATE"),
	test_keyword_window => (Window, "WINDOW"),
	test_keyword_namespace => (Namespace, "NAMESPACE"),
	test_keyword_series => (Series, "SERIES"),
	test_keyword_subscription => (Subscription, "SUBSCRIPTION"),
	test_keyword_table => (Table, "TABLE"),
	test_keyword_ringbuffer => (Ringbuffer, "RINGBUFFER"),
	test_keyword_column => (Column, "COLUMN"),
	test_keyword_policy => (Policy, "POLICY"),
	test_keyword_property => (Property, "PROPERTY"),
	test_keyword_view => (View, "VIEW"),
	test_keyword_deferred => (Deferred, "DEFERRED"),
	test_keyword_transactional => (Transactional, "TRANSACTIONAL"),
	test_keyword_cast => (Cast, "CAST"),
	test_keyword_index => (Index, "INDEX"),
	test_keyword_unique => (Unique, "UNIQUE"),
	test_keyword_primary => (Primary, "PRIMARY"),
	test_keyword_key => (Key, "KEY"),
	test_keyword_asc => (Asc, "ASC"),
	test_keyword_desc => (Desc, "DESC"),
	test_keyword_sequence => (Sequence, "SEQUENCE"),
	test_keyword_alter => (Alter, "ALTER"),
	test_keyword_value => (Value, "VALUE"),
	test_keyword_exists => (Exists, "EXISTS"),
	test_keyword_replace => (Replace, "REPLACE"),
	test_keyword_cascade => (Cascade, "CASCADE"),
	test_keyword_restrict => (Restrict, "RESTRICT"),
	test_keyword_to => (To, "TO"),
	test_keyword_rename => (Rename, "RENAME"),
	test_keyword_rownum => (Rownum, "ROWNUM"),
	test_keyword_dictionary => (Dictionary, "DICTIONARY"),
	test_keyword_for => (For, "FOR"),
	test_keyword_output => (Output, "OUTPUT"),
	test_keyword_append => (Append, "APPEND"),
	test_keyword_assert => (Assert, "ASSERT"),
	test_keyword_patch => (Patch, "PATCH"),
	test_keyword_enum => (Enum, "ENUM"),
	test_keyword_match => (Match, "MATCH"),
	test_keyword_procedure => (Procedure, "PROCEDURE"),
	test_keyword_user => (User, "USER"),
	test_keyword_role => (Role, "ROLE"),
	test_keyword_grant => (Grant, "GRANT"),
	test_keyword_revoke => (Revoke, "REVOKE"),
	test_keyword_require => (Require, "REQUIRE"),
	test_keyword_enable => (Enable, "ENABLE"),
	test_keyword_disable => (Disable, "DISABLE"),
	test_keyword_function => (Function, "FUNCTION"),
	test_keyword_session => (Session, "SESSION"),
	test_keyword_feature => (Feature, "FEATURE"),
	test_keyword_authentication => (Authentication, "AUTHENTICATION"),
	test_keyword_contains => (Contains, "CONTAINS"),
	test_keyword_test => (Test, "TEST"),
	test_keyword_tests => (Tests, "TESTS"),
	test_keyword_run => (Run, "RUN"),
	test_keyword_source => (Source, "SOURCE"),
	test_keyword_sink => (Sink, "SINK"),
	}

	fn check_no_keyword(repr: &str) {
		let bump = Bump::new();
		// Test that keywords with additional characters are not parsed
		// as keywords For example, "map123" should be an identifier,
		// not the MAP keyword
		let test_cases = vec![
			format!("{repr}_something_else"), // e.g., "map_something_else"
			format!("{repr}SomethingElse"),   // e.g., "mapSomethingElse"
			format!("{repr}123"),             // e.g., "map123"
			format!("_{repr}"),               // e.g., "_map"
		];

		for input_str in test_cases {
			let input = format!("{input_str} rest");
			let tokens = tokenize(&bump, &input).unwrap();
			assert!(tokens.len() >= 1);
			// The first token should be an identifier, not a
			// keyword
			assert_eq!(
				tokens[0].kind,
				TokenKind::Identifier,
				"Input '{}' should produce an identifier, not a keyword",
				input_str
			);
			assert_eq!(tokens[0].fragment.text(), &input_str);
		}

		// Also test that the bare lowercase word IS parsed as a keyword
		// (since keywords are case-insensitive)
		let input = format!("{repr} rest");
		let tokens = tokenize(&bump, &input).unwrap();
		assert!(tokens.len() >= 2);
		// In a case-insensitive system, "map" should be parsed as the
		// MAP keyword
		assert!(
			matches!(tokens[0].kind, TokenKind::Keyword(_)),
			"Input '{}' should be parsed as a keyword in case-insensitive mode",
			repr
		);
	}

	macro_rules! generate_not_keyword_tests {
        ($($name:ident => ($repr:literal)),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    check_no_keyword($repr);
                }
            )*
        };
    }

	generate_not_keyword_tests! {
	test_not_keyword_map => ( "map"),
	test_not_keyword_apply => ( "apply"),
	test_not_keyword_by => ( "by"),
	test_not_keyword_from => ( "from"),
	test_not_keyword_aggregate => ( "aggregate"),
	test_not_keyword_sort => ( "sort"),
	test_not_keyword_take => ( "take"),
	test_not_keyword_insert => ( "insert"),
	test_not_keyword_update => ( "update"),
	test_not_keyword_set => ( "set"),
	test_not_keyword_delete => ( "delete"),
	test_not_keyword_left => ( "left"),
	test_not_keyword_inner => ( "inner"),
	test_not_keyword_natural => ( "natural"),
	test_not_keyword_join => ( "join"),
	test_not_keyword_on => ( "on"),
	test_not_keyword_using => ( "using"),
	test_not_keyword_let => ( "let"),
	test_not_keyword_if => ( "if"),
	test_not_keyword_else => ( "else"),
	test_not_keyword_loop => ( "loop"),
	test_not_keyword_while => ( "while"),
	test_not_keyword_break => ( "break"),
	test_not_keyword_continue => ( "continue"),
	test_not_keyword_return => ( "return"),
	test_not_keyword_udf => ( "udf"),
	test_not_keyword_call => ( "call"),
	test_not_keyword_describe => ( "describe"),
	test_not_keyword_create => ( "create"),
	test_not_keyword_drop => ( "drop"),
	test_not_keyword_in => ( "in"),
	test_not_keyword_between => ( "between"),
	test_not_keyword_is => ( "is"),
	test_not_keyword_with => ( "with"),
	test_not_keyword_filter => ( "filter"),
	test_not_keyword_window => ( "window"),
	test_not_keyword_namespace => ( "namespace"),
	test_not_keyword_series => ( "series"),
	test_not_keyword_subscription => ( "subscription"),
	test_not_keyword_table => ( "table"),
	test_not_keyword_ringbuffer => ( "ringbuffer"),
	test_not_keyword_column => ( "column"),
	test_not_keyword_policy => ( "policy"),
	test_not_keyword_view => ( "view"),
	test_not_keyword_deferred => ( "deferred"),
	test_not_keyword_transactional => ( "transactional"),
	test_not_keyword_cast => ( "cast"),
	test_not_keyword_index => ("index"),
	test_not_keyword_unique => ( "unique"),
	test_not_keyword_primary => ("primary"),
	test_not_keyword_key => ("key"),
	test_not_keyword_asc => ("asc"),
	test_not_keyword_desc => ( "desc"),
	test_not_keyword_sequence => ( "sequence"),
	test_not_keyword_alter => ( "alter"),
	test_not_keyword_value => ( "value"),
	test_not_keyword_exists => ( "exists"),
	test_not_keyword_replace => ( "replace"),
	test_not_keyword_cascade => ( "cascade"),
	test_not_keyword_restrict => ( "restrict"),
	test_not_keyword_to => ( "to"),
	test_not_keyword_rename => ( "rename"),
	test_not_keyword_rownum => ( "rownum"),
	test_not_keyword_dictionary => ( "dictionary"),
	test_not_keyword_for => ( "for"),
	test_not_keyword_output => ( "output"),
	test_not_keyword_append => ( "append"),
	test_not_keyword_assert => ( "assert"),
	test_not_keyword_patch => ( "patch"),
	test_not_keyword_enum => ( "enum"),
	test_not_keyword_match => ( "match"),
	test_not_keyword_procedure => ( "procedure"),
	test_not_keyword_user => ( "user"),
	test_not_keyword_role => ( "role"),
	test_not_keyword_grant => ( "grant"),
	test_not_keyword_revoke => ( "revoke"),
	test_not_keyword_require => ( "require"),
	test_not_keyword_enable => ( "enable"),
	test_not_keyword_disable => ( "disable"),
	test_not_keyword_function => ( "function"),
	test_not_keyword_session => ( "session"),
	test_not_keyword_feature => ( "feature"),
	test_not_keyword_authentication => ( "authentication"),
	test_not_keyword_contains => ( "contains"),
	test_not_keyword_test => ( "test"),
	test_not_keyword_tests => ( "tests"),
	test_not_keyword_run => ( "run"),
	test_not_keyword_source => ( "source"),
	test_not_keyword_sink => ( "sink"),
	}

	#[test]
	fn test_select_is_not_keyword() {
		let bump = Bump::new();
		for input in ["SELECT rest", "select rest"] {
			let tokens = tokenize(&bump, input).unwrap();
			assert!(tokens.len() >= 2);
			assert_eq!(
				tokens[0].kind,
				TokenKind::Identifier,
				"Input '{}' should produce an identifier, not a keyword",
				input
			);
		}
	}
}
