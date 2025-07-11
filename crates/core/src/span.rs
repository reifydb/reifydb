// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// Trait to provide a `Span` either directly or lazily (via closure).
pub trait IntoSpan {
    fn into_span(self) -> Span;
}

impl IntoSpan for Span {
    fn into_span(self) -> Span {
        self
    }
}

impl IntoSpan for &Span {
    fn into_span(self) -> Span {
        self.clone()
    }
}

impl<F> IntoSpan for F
where
    F: Fn() -> Span,
{
    fn into_span(self) -> Span {
        self()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    /// The offset represents the position of the fragment relatively to
    /// the input of the parser. It starts at offset 0.
    pub offset: Offset,
    /// The line number of the fragment relatively to the input of the
    /// parser. It starts at line 1.
    pub line: Line,

    pub fragment: String,
}

impl AsRef<str> for Span {
    fn as_ref(&self) -> &str {
        self.fragment.as_str()
    }
}

impl Display for Span {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.fragment, f)
    }
}

impl Span {
    pub fn testing_empty() -> Self {
        Self { offset: Offset(0), line: Line(1), fragment: "".to_string() }
    }

    pub fn testing(s: impl Into<String>) -> Self {
        Self { offset: Offset(0), line: Line(1), fragment: s.into() }
    }
}

impl PartialOrd for Span {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Span {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset).then(self.line.cmp(&other.line))
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Offset(pub u32);

impl PartialEq<i32> for Offset {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other as u32
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Line(pub u32);

impl PartialEq<i32> for Line {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other as u32
    }
}

impl Span {
    /// Merge multiple spans (in any order) into one encompassing span.
    pub fn merge_all(spans: impl IntoIterator<Item = Span>) -> Span {
        let mut spans: Vec<Span> = spans.into_iter().collect();
        assert!(!spans.is_empty());

        spans.sort();

        let first = spans.first().unwrap();

        let mut fragment = String::with_capacity(spans.iter().map(|s| s.fragment.len()).sum());
        for span in &spans {
            fragment.push_str(&span.fragment);
        }

        Span { offset: first.offset, line: first.line, fragment }
    }
}

#[cfg(test)]
mod tests {
    mod merge_all {
        use crate::{Line, Offset, Span};

        #[test]
        fn test_multiple_spans_in_order() {
            let spans = vec![
                Span { offset: Offset(0), line: Line(1), fragment: "hello ".into() },
                Span { offset: Offset(6), line: Line(1), fragment: "world".into() },
            ];

            let merged = Span::merge_all(spans);

            assert_eq!(merged.offset, Offset(0));
            assert_eq!(merged.line, Line(1));
            assert_eq!(merged.fragment, "hello world");
        }

        #[test]
        fn test_multiple_spans_out_of_order() {
            let spans = vec![
                Span { offset: Offset(10), line: Line(1), fragment: "world".into() },
                Span { offset: Offset(0), line: Line(1), fragment: "hello ".into() },
            ];

            let merged = Span::merge_all(spans);

            assert_eq!(merged.offset, Offset(0));
            assert_eq!(merged.fragment, "hello world");
        }

        #[test]
        fn test_single_span_returns_same() {
            let span = Span { offset: Offset(5), line: Line(3), fragment: "solo".into() };

            let merged = Span::merge_all([span.clone()]);

            assert_eq!(merged, span);
        }

        #[test]
        fn test_merge_three_spans_out_of_order() {
            let span1 = Span { offset: Offset(10), line: Line(1), fragment: "world".into() };
            let span2 = Span { offset: Offset(0), line: Line(1), fragment: "hello ".into() };
            let span3 = Span { offset: Offset(6), line: Line(1), fragment: "beautiful ".into() };

            let merged = Span::merge_all([span1, span2, span3]);

            assert_eq!(merged.offset, Offset(0));
            assert_eq!(merged.line, Line(1));
            assert_eq!(merged.fragment, "hello beautiful world");
        }

        #[test]
        fn test_overlapping_spans() {
            let spans = vec![
                Span { offset: Offset(0), line: Line(1), fragment: "abc".into() },
                Span { offset: Offset(2), line: Line(1), fragment: "cde".into() },
            ];

            let merged = Span::merge_all(spans);

            assert_eq!(merged.offset, Offset(0));
            assert_eq!(merged.fragment, "abccde");
        }
    }
}
