// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::frame::build_indices;
use crate::frame::{
    ColumnQualified, ColumnValues, Frame, FrameColumn, FrameColumnLayout, FullyQualified,
    TableQualified, Unqualified,
};

#[derive(Debug, Clone)]
pub struct FrameLayout {
    pub columns: Vec<FrameColumnLayout>,
}

impl FrameLayout {
    pub fn from_frame(frame: &Frame) -> Self {
        Self { columns: frame.columns.iter().map(|c| FrameColumnLayout::from_column(c)).collect() }
    }
}

impl Frame {
    pub fn apply_layout(&mut self, layout: &FrameLayout) {
        // Check for duplicate column names and qualify them only when needed
        let layout_with_qualification = self.qualify_duplicates_only(layout);

        for (i, column_layout) in layout_with_qualification.columns.iter().enumerate() {
            if i < self.columns.len() {
                let column = &mut self.columns[i];
                let values = std::mem::replace(column.values_mut(), ColumnValues::undefined(0));

                *column = match (&column_layout.schema, &column_layout.table) {
                    (Some(schema), Some(table)) => FrameColumn::FullyQualified(FullyQualified {
                        schema: schema.clone(),
                        table: table.clone(),
                        name: column_layout.name.clone(),
                        values,
                    }),
                    (None, Some(table)) => FrameColumn::TableQualified(TableQualified {
                        table: table.clone(),
                        name: column_layout.name.clone(),
                        values,
                    }),
                    (None, None) => match column {
                        FrameColumn::Unqualified(_) => FrameColumn::Unqualified(Unqualified {
                            name: column_layout.name.clone(),
                            values,
                        }),
                        _ => FrameColumn::ColumnQualified(ColumnQualified {
                            name: column_layout.name.clone(),
                            values,
                        }),
                    },
                    (Some(_), None) => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column_layout.name.clone(),
                        values,
                    }),
                };
            }
        }

        let (index, frame_index) = build_indices(&self.columns);
        self.index = index;
        self.frame_index = frame_index;
    }

    fn qualify_duplicates_only(&self, layout: &FrameLayout) -> FrameLayout {
        use std::collections::HashMap;

        // Group columns by name and check for ambiguity across different table/schema contexts
        let mut name_groups: HashMap<String, Vec<(Option<String>, Option<String>)>> = HashMap::new();
        for column_layout in &layout.columns {
            name_groups.entry(column_layout.name.clone())
                .or_insert_with(Vec::new)
                .push((column_layout.schema.clone(), column_layout.table.clone()));
        }

        // Only qualify columns that appear more than once across different table/schema contexts
        let qualified_columns: Vec<_> = layout
            .columns
            .iter()
            .map(|column_layout| {
                let contexts = name_groups.get(&column_layout.name).unwrap();
                
                // Check if this column name appears in different table/schema contexts
                let mut unique_contexts = std::collections::HashSet::new();
                for (schema, table) in contexts {
                    unique_contexts.insert((schema.clone(), table.clone()));
                }
                
                // Only qualify if there are actually multiple different contexts for this column name
                let has_duplicates = unique_contexts.len() > 1;

                if has_duplicates {
                    // This column has naming conflicts - add qualification using available table info
                    match (&column_layout.schema, &column_layout.table) {
                        (Some(schema), Some(table)) => FrameColumnLayout {
                            schema: Some(schema.clone()),
                            table: Some(table.clone()),
                            name: column_layout.name.clone(),
                        },
                        (None, Some(table)) => FrameColumnLayout {
                            schema: None,
                            table: Some(table.clone()),
                            name: column_layout.name.clone(),
                        },
                        _ => {
                            // No table info in layout, try to get it from existing columns
                            if let Some(existing_column) =
                                self.columns.iter().find(|c| c.name() == column_layout.name)
                            {
                                match (existing_column.schema(), existing_column.table()) {
                                    (Some(schema), Some(table)) => FrameColumnLayout {
                                        schema: Some(schema.to_string()),
                                        table: Some(table.to_string()),
                                        name: column_layout.name.clone(),
                                    },
                                    (None, Some(table)) => FrameColumnLayout {
                                        schema: None,
                                        table: Some(table.to_string()),
                                        name: column_layout.name.clone(),
                                    },
                                    _ => {
                                        // Use frame name as fallback table qualification
                                        FrameColumnLayout {
                                            schema: None,
                                            table: Some(self.name.clone()),
                                            name: column_layout.name.clone(),
                                        }
                                    }
                                }
                            } else {
                                // Use frame name as fallback table qualification
                                FrameColumnLayout {
                                    schema: None,
                                    table: Some(self.name.clone()),
                                    name: column_layout.name.clone(),
                                }
                            }
                        }
                    }
                } else {
                    // No duplicates - remove unnecessary qualification
                    FrameColumnLayout {
                        schema: None,
                        table: None,
                        name: column_layout.name.clone(),
                    }
                }
            })
            .collect();

        FrameLayout { columns: qualified_columns }
    }
}
