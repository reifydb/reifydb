// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{Span, SpanColumn, SpanLine};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedSpan {
    /// The offset represents the position of the fragment relatively to
    /// the input of the parser. It starts at offset 0.
    pub column: SpanColumn,
    /// The line number of the fragment relatively to the input of the
    /// parser. It starts at line 1.
    pub line: SpanLine,

    pub fragment: String,
}

impl AsRef<str> for OwnedSpan {
    fn as_ref(&self) -> &str {
        self.fragment.as_str()
    }
}

impl Display for OwnedSpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.fragment, f)
    }
}

impl OwnedSpan {
    pub fn testing_empty() -> Self {
        Self { column: SpanColumn(0), line: SpanLine(1), fragment: "".to_string() }
    }

    pub fn testing(s: impl Into<String>) -> Self {
        Self { column: SpanColumn(0), line: SpanLine(1), fragment: s.into() }
    }

    /// Merge multiple spans (in any order) into one encompassing span.
    pub fn merge_all(spans: impl IntoIterator<Item = OwnedSpan>) -> OwnedSpan {
        let mut spans: Vec<OwnedSpan> = spans.into_iter().collect();
        assert!(!spans.is_empty());

        spans.sort();

        let first = spans.first().unwrap();

        let mut fragment = String::with_capacity(spans.iter().map(|s| s.fragment.len()).sum());
        for span in &spans {
            fragment.push_str(&span.fragment);
        }

        OwnedSpan { column: first.column, line: first.line, fragment }
    }

    /// Split this span by delimiter, returning a vector of spans for each part.
    /// Each resulting span will have the correct column offset and fragment.
    pub fn split(&self, delimiter: char) -> Vec<OwnedSpan> {
        let parts: Vec<&str> = self.fragment.split(delimiter).collect();
        let mut result = Vec::new();
        let mut current_column = self.column.0;

        for part in parts {
            let part_span = OwnedSpan {
                column: SpanColumn(current_column),
                line: self.line,
                fragment: part.to_string(),
            };
            result.push(part_span);
            // Move column forward by part length + 1 (for the delimiter)
            current_column += part.len() as u32 + 1;
        }

        result
    }

    /// Get a sub-span starting at the given offset with the given length.
    /// Useful for pinpointing specific parts of a larger span.
    pub fn sub_span(&self, offset: usize, length: usize) -> OwnedSpan {
        let end = std::cmp::min(offset + length, self.fragment.len());
        let fragment = if offset < self.fragment.len() {
            self.fragment[offset..end].to_string()
        } else {
            String::new()
        };

        OwnedSpan { column: SpanColumn(self.column.0 + offset as u32), line: self.line, fragment }
    }
}

impl Span for OwnedSpan {
    type SubSpan = OwnedSpan;

    fn fragment(&self) -> &str {
        &self.fragment
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
            let part_span = OwnedSpan {
                column: SpanColumn(current_column),
                line: self.line,
                fragment: part.to_string(),
            };
            result.push(part_span);
            // Move column forward by part length + 1 (for the delimiter)
            current_column += part.len() as u32 + 1;
        }

        result
    }

    fn to_owned(self) -> OwnedSpan {
        self
    }

    fn sub_span(&self, offset: usize, length: usize) -> Self::SubSpan {
        let end = std::cmp::min(offset + length, self.fragment.len());
        let fragment = if offset < self.fragment.len() {
            self.fragment[offset..end].to_string()
        } else {
            String::new()
        };

        OwnedSpan { column: SpanColumn(self.column.0 + offset as u32), line: self.line, fragment }
    }
}

impl Span for &OwnedSpan {
    type SubSpan = OwnedSpan;

    fn fragment(&self) -> &str {
        &self.fragment
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
            let part_span = OwnedSpan {
                column: SpanColumn(current_column),
                line: self.line,
                fragment: part.to_string(),
            };
            result.push(part_span);
            // Move column forward by part length + 1 (for the delimiter)
            current_column += part.len() as u32 + 1;
        }

        result
    }

    fn to_owned(self) -> OwnedSpan {
        (*self).clone()
    }

    fn sub_span(&self, offset: usize, length: usize) -> Self::SubSpan {
        let end = std::cmp::min(offset + length, self.fragment.len());
        let fragment = if offset < self.fragment.len() {
            self.fragment[offset..end].to_string()
        } else {
            String::new()
        };

        OwnedSpan { column: SpanColumn(self.column.0 + offset as u32), line: self.line, fragment }
    }
}

impl PartialOrd for OwnedSpan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OwnedSpan {
    fn cmp(&self, other: &Self) -> Ordering {
        self.column.cmp(&other.column).then(self.line.cmp(&other.line))
    }
}

#[cfg(test)]
mod tests {
    mod merge_all {
        use crate::{OwnedSpan, SpanColumn, SpanLine};

        #[test]
        fn test_multiple_spans_in_order() {
            let spans = vec![
                OwnedSpan { column: SpanColumn(0), line: SpanLine(1), fragment: "hello ".into() },
                OwnedSpan { column: SpanColumn(6), line: SpanLine(1), fragment: "world".into() },
            ];

            let merged = OwnedSpan::merge_all(spans);

            assert_eq!(merged.column, SpanColumn(0));
            assert_eq!(merged.line, SpanLine(1));
            assert_eq!(merged.fragment, "hello world");
        }

        #[test]
        fn test_multiple_spans_out_of_order() {
            let spans = vec![
                OwnedSpan { column: SpanColumn(10), line: SpanLine(1), fragment: "world".into() },
                OwnedSpan { column: SpanColumn(0), line: SpanLine(1), fragment: "hello ".into() },
            ];

            let merged = OwnedSpan::merge_all(spans);

            assert_eq!(merged.column, SpanColumn(0));
            assert_eq!(merged.fragment, "hello world");
        }

        #[test]
        fn test_single_span_returns_same() {
            let span =
                OwnedSpan { column: SpanColumn(5), line: SpanLine(3), fragment: "solo".into() };

            let merged = OwnedSpan::merge_all([span.clone()]);

            assert_eq!(merged, span);
        }

        #[test]
        fn test_merge_three_spans_out_of_order() {
            let span1 =
                OwnedSpan { column: SpanColumn(10), line: SpanLine(1), fragment: "world".into() };
            let span2 =
                OwnedSpan { column: SpanColumn(0), line: SpanLine(1), fragment: "hello ".into() };
            let span3 = OwnedSpan {
                column: SpanColumn(6),
                line: SpanLine(1),
                fragment: "beautiful ".into(),
            };

            let merged = OwnedSpan::merge_all([span1, span2, span3]);

            assert_eq!(merged.column, SpanColumn(0));
            assert_eq!(merged.line, SpanLine(1));
            assert_eq!(merged.fragment, "hello beautiful world");
        }

        #[test]
        fn test_overlapping_spans() {
            let spans = vec![
                OwnedSpan { column: SpanColumn(0), line: SpanLine(1), fragment: "abc".into() },
                OwnedSpan { column: SpanColumn(2), line: SpanLine(1), fragment: "cde".into() },
            ];

            let merged = OwnedSpan::merge_all(spans);

            assert_eq!(merged.column, SpanColumn(0));
            assert_eq!(merged.fragment, "abccde");
        }
    }

    mod split {
        use crate::{OwnedSpan, SpanColumn, SpanLine};

        #[test]
        fn test_split_date() {
            let span = OwnedSpan {
                column: SpanColumn(10),
                line: SpanLine(1),
                fragment: "2024-03-15".to_string(),
            };

            let parts = span.split('-');

            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0].fragment, "2024");
            assert_eq!(parts[0].column, SpanColumn(10));
            assert_eq!(parts[1].fragment, "03");
            assert_eq!(parts[1].column, SpanColumn(15)); // 10 + 4 + 1
            assert_eq!(parts[2].fragment, "15");
            assert_eq!(parts[2].column, SpanColumn(18)); // 10 + 4 + 1 + 2 + 1
        }

        #[test]
        fn test_split_time() {
            let span = OwnedSpan {
                column: SpanColumn(0),
                line: SpanLine(1),
                fragment: "14:30:45".to_string(),
            };

            let parts = span.split(':');

            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0].fragment, "14");
            assert_eq!(parts[0].column, SpanColumn(0));
            assert_eq!(parts[1].fragment, "30");
            assert_eq!(parts[1].column, SpanColumn(3)); // 0 + 2 + 1
            assert_eq!(parts[2].fragment, "45");
            assert_eq!(parts[2].column, SpanColumn(6)); // 0 + 2 + 1 + 2 + 1
        }

        #[test]
        fn test_split_single_part() {
            let span = OwnedSpan {
                column: SpanColumn(5),
                line: SpanLine(2),
                fragment: "single".to_string(),
            };

            let parts = span.split('-');

            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].fragment, "single");
            assert_eq!(parts[0].column, SpanColumn(5));
            assert_eq!(parts[0].line, SpanLine(2));
        }
    }

    mod sub_span {
        use crate::{OwnedSpan, SpanColumn, SpanLine};

        #[test]
        fn test_sub_span_middle() {
            let span = OwnedSpan {
                column: SpanColumn(0),
                line: SpanLine(1),
                fragment: "2024-03-15".to_string(),
            };

            let sub = span.sub_span(5, 2); // Extract "03"

            assert_eq!(sub.fragment, "03");
            assert_eq!(sub.column, SpanColumn(5));
            assert_eq!(sub.line, SpanLine(1));
        }

        #[test]
        fn test_sub_span_bounds() {
            let span = OwnedSpan {
                column: SpanColumn(10),
                line: SpanLine(1),
                fragment: "abc".to_string(),
            };

            let sub = span.sub_span(1, 10); // Request beyond string length

            assert_eq!(sub.fragment, "bc");
            assert_eq!(sub.column, SpanColumn(11));
        }

        #[test]
        fn test_sub_span_out_of_bounds() {
            let span =
                OwnedSpan { column: SpanColumn(0), line: SpanLine(1), fragment: "abc".to_string() };

            let sub = span.sub_span(10, 5); // Start beyond string length

            assert_eq!(sub.fragment, "");
            assert_eq!(sub.column, SpanColumn(10));
        }
    }
}
