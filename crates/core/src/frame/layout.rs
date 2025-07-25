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

        // Count occurrences of each column name
        let mut name_counts: HashMap<String, usize> = HashMap::new();
        for column_layout in &layout.columns {
            *name_counts.entry(column_layout.name.clone()).or_insert(0) += 1;
        }

        // Only qualify columns that appear more than once
        let qualified_columns: Vec<_> = layout
            .columns
            .iter()
            .map(|column_layout| {
                let has_duplicates = name_counts.get(&column_layout.name).copied().unwrap_or(0) > 1;

                if has_duplicates && column_layout.schema.is_none() && column_layout.table.is_none()
                {
                    // This column has duplicates and is unqualified - add qualification
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
                                // Use frame name as table qualification
                                FrameColumnLayout {
                                    schema: None,
                                    table: Some(self.name.clone()),
                                    name: column_layout.name.clone(),
                                }
                            }
                        }
                    } else {
                        // Use frame name as table qualification
                        FrameColumnLayout {
                            schema: None,
                            table: Some(self.name.clone()),
                            name: column_layout.name.clone(),
                        }
                    }
                } else {
                    // No duplicates or already qualified - keep as dictated by layout
                    column_layout.clone()
                }
            })
            .collect();

        FrameLayout { columns: qualified_columns }
    }
}
