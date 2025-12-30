// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Source location for error reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
	/// Byte offset from start.
	pub start: usize,
	/// Byte offset (exclusive).
	pub end: usize,
	/// Line number (1-indexed).
	pub line: u32,
	/// Column number (1-indexed).
	pub column: u32,
}

impl Span {
	pub fn new(start: usize, end: usize, line: u32, column: u32) -> Self {
		Self {
			start,
			end,
			line,
			column,
		}
	}

	/// Merge two spans into one that covers both.
	pub fn merge(&self, other: &Span) -> Span {
		Span {
			start: self.start.min(other.start),
			end: self.end.max(other.end),
			line: self.line.min(other.line),
			column: if self.line <= other.line {
				self.column
			} else {
				other.column
			},
		}
	}
}

/// A token with its kind and source location.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
	pub kind: TokenKind,
	pub span: Span,
	/// Original text for error messages.
	pub text: String,
}

impl Token {
	pub fn new(kind: TokenKind, span: Span, text: String) -> Self {
		Self {
			kind,
			span,
			text,
		}
	}
}

/// Token kinds.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
	// End of input
	Eof,

	// Identifiers and literals
	/// Column names, table names.
	Ident,
	/// Integer literal: 123, -456.
	Int(i64),
	/// Float literal: 1.5, -3.14.
	Float(f64),
	/// String literal: "hello", 'world'.
	String(String),
	/// Boolean literal: true, false.
	Bool(bool),

	// Keywords (pipeline stages)
	/// scan keyword.
	Scan,
	/// filter keyword.
	Filter,
	/// select keyword.
	Select,
	/// take keyword.
	Take,
	/// sort keyword.
	Sort,
	/// extend keyword (project computed columns).
	Extend,

	// Keywords (control flow and definitions)
	/// let keyword.
	Let,
	/// def keyword.
	Def,
	/// if keyword.
	If,
	/// else keyword.
	Else,
	/// inline keyword (empty pipeline).
	Inline,
	/// loop keyword.
	Loop,
	/// break keyword.
	Break,
	/// continue keyword.
	Continue,
	/// for keyword.
	For,
	/// in keyword.
	In,
	/// exists keyword (for subquery EXISTS checks).
	Exists,

	// Operators - Comparison
	/// ==
	Eq,
	/// !=
	Ne,
	/// <
	Lt,
	/// <=
	Le,
	/// >
	Gt,
	/// >=
	Ge,

	// Operators - Logical
	/// and, &&
	And,
	/// or, ||
	Or,
	/// not, !
	Not,

	// Operators - Arithmetic
	/// +
	Plus,
	/// -
	Minus,
	/// *
	Star,
	/// /
	Slash,

	// Delimiters
	/// |
	Pipe,
	/// (
	LParen,
	/// )
	RParen,
	/// [
	LBracket,
	/// ]
	RBracket,
	/// {
	LBrace,
	/// }
	RBrace,
	/// ,
	Comma,
	/// :
	Colon,
	/// $ (variable prefix)
	Dollar,
	/// = (single equals for assignment)
	Assign,
	/// . (field access)
	Dot,
	/// :: (module path separator)
	ColonColon,

	// Special
	/// null, undefined.
	Null,
	/// as (for aliases).
	As,
	/// asc (for sort order).
	Asc,
	/// desc (for sort order).
	Desc,
}

impl TokenKind {
	/// Try to match an identifier string to a keyword.
	pub fn from_keyword(s: &str) -> Option<TokenKind> {
		match s.to_lowercase().as_str() {
			"scan" => Some(TokenKind::Scan),
			"filter" => Some(TokenKind::Filter),
			"select" => Some(TokenKind::Select),
			"take" => Some(TokenKind::Take),
			"sort" => Some(TokenKind::Sort),
			"extend" => Some(TokenKind::Extend),
			"let" => Some(TokenKind::Let),
			"def" => Some(TokenKind::Def),
			"if" => Some(TokenKind::If),
			"else" => Some(TokenKind::Else),
			"inline" => Some(TokenKind::Inline),
			"loop" => Some(TokenKind::Loop),
			"break" => Some(TokenKind::Break),
			"continue" => Some(TokenKind::Continue),
			"for" => Some(TokenKind::For),
			"in" => Some(TokenKind::In),
			"exists" => Some(TokenKind::Exists),
			"and" => Some(TokenKind::And),
			"or" => Some(TokenKind::Or),
			"not" => Some(TokenKind::Not),
			"true" => Some(TokenKind::Bool(true)),
			"false" => Some(TokenKind::Bool(false)),
			"null" | "undefined" => Some(TokenKind::Null),
			"as" => Some(TokenKind::As),
			"asc" => Some(TokenKind::Asc),
			"desc" => Some(TokenKind::Desc),
			_ => None,
		}
	}

	/// Check if this token kind can start a stage.
	pub fn is_stage_keyword(&self) -> bool {
		matches!(
			self,
			TokenKind::Scan
				| TokenKind::Filter | TokenKind::Select
				| TokenKind::Take | TokenKind::Sort
				| TokenKind::Extend
		)
	}

	/// Check if this token kind is a binary comparison operator.
	pub fn is_comparison_op(&self) -> bool {
		matches!(
			self,
			TokenKind::Eq | TokenKind::Ne | TokenKind::Lt | TokenKind::Le | TokenKind::Gt | TokenKind::Ge
		)
	}

	/// Check if this token kind is a binary logical operator.
	pub fn is_logical_op(&self) -> bool {
		matches!(self, TokenKind::And | TokenKind::Or)
	}

	/// Check if this token kind is an arithmetic operator.
	pub fn is_arithmetic_op(&self) -> bool {
		matches!(self, TokenKind::Plus | TokenKind::Minus | TokenKind::Star | TokenKind::Slash)
	}
}

impl std::fmt::Display for TokenKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TokenKind::Eof => write!(f, "end of input"),
			TokenKind::Ident => write!(f, "identifier"),
			TokenKind::Int(_) => write!(f, "integer"),
			TokenKind::Float(_) => write!(f, "float"),
			TokenKind::String(_) => write!(f, "string"),
			TokenKind::Bool(_) => write!(f, "boolean"),
			TokenKind::Scan => write!(f, "scan"),
			TokenKind::Filter => write!(f, "filter"),
			TokenKind::Select => write!(f, "select"),
			TokenKind::Take => write!(f, "take"),
			TokenKind::Sort => write!(f, "sort"),
			TokenKind::Extend => write!(f, "extend"),
			TokenKind::Let => write!(f, "let"),
			TokenKind::Def => write!(f, "def"),
			TokenKind::If => write!(f, "if"),
			TokenKind::Else => write!(f, "else"),
			TokenKind::Inline => write!(f, "inline"),
			TokenKind::Loop => write!(f, "loop"),
			TokenKind::Break => write!(f, "break"),
			TokenKind::Continue => write!(f, "continue"),
			TokenKind::For => write!(f, "for"),
			TokenKind::In => write!(f, "in"),
			TokenKind::Exists => write!(f, "exists"),
			TokenKind::Eq => write!(f, "=="),
			TokenKind::Ne => write!(f, "!="),
			TokenKind::Lt => write!(f, "<"),
			TokenKind::Le => write!(f, "<="),
			TokenKind::Gt => write!(f, ">"),
			TokenKind::Ge => write!(f, ">="),
			TokenKind::And => write!(f, "and"),
			TokenKind::Or => write!(f, "or"),
			TokenKind::Not => write!(f, "not"),
			TokenKind::Plus => write!(f, "+"),
			TokenKind::Minus => write!(f, "-"),
			TokenKind::Star => write!(f, "*"),
			TokenKind::Slash => write!(f, "/"),
			TokenKind::Pipe => write!(f, "|"),
			TokenKind::LParen => write!(f, "("),
			TokenKind::RParen => write!(f, ")"),
			TokenKind::LBracket => write!(f, "["),
			TokenKind::RBracket => write!(f, "]"),
			TokenKind::LBrace => write!(f, "{{"),
			TokenKind::RBrace => write!(f, "}}"),
			TokenKind::Comma => write!(f, ","),
			TokenKind::Colon => write!(f, ":"),
			TokenKind::Dollar => write!(f, "$"),
			TokenKind::Assign => write!(f, "="),
			TokenKind::Dot => write!(f, "."),
			TokenKind::ColonColon => write!(f, "::"),
			TokenKind::Null => write!(f, "null"),
			TokenKind::As => write!(f, "as"),
			TokenKind::Asc => write!(f, "asc"),
			TokenKind::Desc => write!(f, "desc"),
		}
	}
}
