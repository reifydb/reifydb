// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

pub use borrowed::BorrowedSpan;
pub use owned::OwnedSpan;

mod borrowed;
mod owned;

// Trait for types that can provide span information for parsing
pub trait Span: Clone {
    type SubSpan: Span + IntoOwnedSpan;

    fn fragment(&self) -> &str;
    fn line(&self) -> SpanLine;
    fn column(&self) -> SpanColumn;

    /// Get the fragment with leading and trailing whitespace trimmed
    fn trimmed_fragment(&self) -> &str {
        self.fragment().trim()
    }

    /// Split this span by delimiter, returning a vector of spans for each part.
    /// For OwnedSpan, returns Vec<OwnedSpan>. For BorrowedSpan, returns Vec<BorrowedSpan>.
    fn split(&self, delimiter: char) -> Vec<Self::SubSpan>;

    /// Convert to owned version
    fn to_owned(self) -> OwnedSpan
    where
        Self: Sized;

    /// Get a sub-span starting at the given offset with the given length.
    /// For OwnedSpan, returns OwnedSpan. For BorrowedSpan, returns BorrowedSpan.
    fn sub_span(&self, offset: usize, length: usize) -> Self::SubSpan;
}

/// Trait to provide a `OwnedSpan` either directly or lazily (via closure).
pub trait IntoOwnedSpan {
    fn into_span(self) -> OwnedSpan;
}

impl IntoOwnedSpan for OwnedSpan {
    fn into_span(self) -> OwnedSpan {
        self
    }
}

impl IntoOwnedSpan for &OwnedSpan {
    fn into_span(self) -> OwnedSpan {
        self.clone()
    }
}

impl<F> IntoOwnedSpan for F
where
    F: Fn() -> OwnedSpan,
{
    fn into_span(self) -> OwnedSpan {
        self()
    }
}

impl<'a> IntoOwnedSpan for BorrowedSpan<'a> {
    fn into_span(self) -> OwnedSpan {
        OwnedSpan { column: self.column, line: self.line, fragment: self.fragment.to_string() }
    }
}

impl<'a> IntoOwnedSpan for &BorrowedSpan<'a> {
    fn into_span(self) -> OwnedSpan {
        OwnedSpan { column: self.column, line: self.line, fragment: self.fragment.to_string() }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SpanColumn(pub u32);

impl PartialEq<i32> for SpanColumn {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other as u32
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SpanLine(pub u32);

impl PartialEq<i32> for SpanLine {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other as u32
    }
}
