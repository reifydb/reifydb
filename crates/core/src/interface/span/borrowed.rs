// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{OwnedSpan, Span, SpanColumn, SpanLine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedSpan<'a> {
    /// The offset represents the position of the fragment relatively to
    /// the input of the parser. It starts at offset 0.
    pub column: SpanColumn,
    /// The line number of the fragment relatively to the input of the
    /// parser. It starts at line 1.
    pub line: SpanLine,

    pub fragment: &'a str,
}

impl<'a> BorrowedSpan<'a> {
    pub fn new(fragment: &'a str) -> Self {
        Self { column: SpanColumn(0), line: SpanLine(1), fragment }
    }

    pub fn with_position(fragment: &'a str, line: SpanLine, column: SpanColumn) -> Self {
        Self { column, line, fragment }
    }
}

impl<'a> Span for BorrowedSpan<'a> {
    type SubSpan = BorrowedSpan<'a>;

    fn fragment(&self) -> &str {
        self.fragment
    }

    fn line(&self) -> SpanLine {
        self.line
    }

    fn column(&self) -> SpanColumn {
        self.column
    }

    fn split(&self, delimiter: char) -> Vec<Self::SubSpan> {
        let parts: Vec<&str> = self.fragment.split(delimiter).collect();
        let mut result = Vec::new();
        let mut current_column = self.column.0;

        for part in parts {
            let part_span = BorrowedSpan {
                column: SpanColumn(current_column),
                line: self.line,
                fragment: part,
            };
            result.push(part_span);
            // Move column forward by part length + 1 (for the delimiter)
            current_column += part.len() as u32 + 1;
        }

        result
    }

    fn to_owned(self) -> OwnedSpan
    where
        Self: Sized,
    {
        OwnedSpan {
            column: self.column(),
            line: self.line(),
            fragment: self.fragment().to_string(),
        }
    }

    fn sub_span(&self, offset: usize, length: usize) -> Self::SubSpan {
        let end = std::cmp::min(offset + length, self.fragment.len());
        let fragment = if offset < self.fragment.len() { &self.fragment[offset..end] } else { "" };

        BorrowedSpan {
            column: SpanColumn(self.column.0 + offset as u32),
            line: self.line,
            fragment,
        }
    }
}

impl<'a> Span for &BorrowedSpan<'a> {
    type SubSpan = BorrowedSpan<'a>;

    fn fragment(&self) -> &str {
        self.fragment
    }

    fn line(&self) -> SpanLine {
        self.line
    }

    fn column(&self) -> SpanColumn {
        self.column
    }

    fn split(&self, delimiter: char) -> Vec<Self::SubSpan> {
        let parts: Vec<&str> = self.fragment.split(delimiter).collect();
        let mut result = Vec::new();
        let mut current_column = self.column.0;

        for part in parts {
            let part_span = BorrowedSpan {
                column: SpanColumn(current_column),
                line: self.line,
                fragment: part,
            };
            result.push(part_span);
            // Move column forward by part length + 1 (for the delimiter)
            current_column += part.len() as u32 + 1;
        }

        result
    }

    fn to_owned(self) -> OwnedSpan
    where
        Self: Sized,
    {
        OwnedSpan {
            column: self.column(),
            line: self.line(),
            fragment: self.fragment().to_string(),
        }
    }

    fn sub_span(&self, offset: usize, length: usize) -> Self::SubSpan {
        let end = std::cmp::min(offset + length, self.fragment.len());
        let fragment = if offset < self.fragment.len() { &self.fragment[offset..end] } else { "" };

        BorrowedSpan {
            column: SpanColumn(self.column.0 + offset as u32),
            line: self.line,
            fragment,
        }
    }
}
