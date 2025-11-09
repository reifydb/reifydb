//! Example: Word Count Operator
//!
//! This stateful operator counts words from text fields.

use reifydb_operator_sdk::prelude::*;
use std::collections::HashMap;

/// A word count operator that maintains running counts
#[derive(Default)]
struct WordCountOperator {
    counts: HashMap<String, u64>,
    text_field: String,
}

impl WordCountOperator {
    /// Create a new word count operator
    pub fn new(text_field: impl Into<String>) -> Self {
        Self {
            counts: HashMap::new(),
            text_field: text_field.into(),
        }
    }
}

impl Operator for WordCountOperator {
    fn initialize(&mut self, config: &[u8]) -> Result<()> {
        // Parse configuration to get the text field name
        if let Ok(field_name) = std::str::from_utf8(config) {
            if !field_name.is_empty() {
                self.text_field = field_name.to_string();
            }
        }
        if self.text_field.is_empty() {
            self.text_field = "text".to_string(); // Default field name
        }
        Ok(())
    }

    fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
        // Load previous counts from state
        if let Some(counts) = ctx.state().get::<HashMap<String, u64>>("word_counts")? {
            self.counts = counts;
        }

        // Process input rows
        for diff in &input.diffs {
            match diff {
                FlowDiff::Insert { post: _ } | FlowDiff::Update { post: _, .. } => {
                    // In a real implementation, we would extract the text field
                    // from the row and count words. For this example, we'll
                    // simulate with some dummy text.
                    let text = "hello world hello";
                    for word in text.split_whitespace() {
                        *self.counts.entry(word.to_string()).or_insert(0) += 1;
                    }
                }
                FlowDiff::Remove { .. } => {
                    // Optionally handle removals by decrementing counts
                }
            }
        }

        // Save updated counts to state
        ctx.state().set("word_counts", &self.counts)?;

        // Build output with current statistics
        let output = FlowChangeBuilder::new()
            .insert(row! {
                number: 0,
                data: {
                    "total_words": self.counts.values().sum::<u64>(),
                    "unique_words": self.counts.len(),
                    "top_word": self.get_top_word()
                }
            })
            .with_version(input.version)
            .build();

        Ok(output)
    }

    fn metadata(&self) -> OperatorMetadata {
        OperatorMetadata {
            name: "word_count",
            version: 1,
            capabilities: Capabilities::new().with_stateful(true),
        }
    }
}

impl WordCountOperator {
    /// Get the most frequent word
    fn get_top_word(&self) -> String {
        self.counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(word, _)| word.clone())
            .unwrap_or_else(|| String::new())
    }
}

// Export the operator for FFI
export_operator!(WordCountOperator);

// Main function for testing - in production, compile as cdylib
fn main() {
    println!("Word count operator example");
    println!("This should be compiled as a dynamic library (cdylib) for use with ReifyDB");
    println!("Use: cargo build --example word_count --release");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_word_count_initialization() {
        let mut operator = WordCountOperator::default();
        let config = b"content";
        operator.initialize(config).unwrap();

        assert_eq!(operator.text_field, "content");
    }

    #[test]
    fn test_word_count_stateful() {
        let mut operator = WordCountOperator::new("text");
        let mut ctx = MockContext::new();

        // First batch
        let input1 = flow_change! {
            insert: { "text": "hello world" },
            version: 1
        };

        let output1 = operator.apply(ctx.as_mut(), input1).unwrap();
        assert_eq!(output1.diffs.len(), 1);

        // Verify state was saved
        assert!(ctx.has_state("word_counts"));

        // Second batch - should accumulate counts
        let input2 = flow_change! {
            insert: { "text": "hello again" },
            version: 2
        };

        let output2 = operator.apply(ctx.as_mut(), input2).unwrap();
        assert_eq!(output2.version, 2);
    }

    #[test]
    fn test_top_word() {
        let mut operator = WordCountOperator::new("text");
        operator.counts.insert("hello".to_string(), 5);
        operator.counts.insert("world".to_string(), 3);
        operator.counts.insert("test".to_string(), 1);

        assert_eq!(operator.get_top_word(), "hello");
    }
}