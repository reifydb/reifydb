// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::{HashMap, HashSet};

use super::{ColumnLayout, ColumnsLayout};
use crate::value::columnar::{
	Column, ColumnData, ColumnQualified, Columns, FullyQualified,
	SourceQualified, Unqualified,
};

impl Columns {
	pub fn apply_layout(&mut self, layout: &ColumnsLayout) {
		// Check for duplicate column names and qualify them only when
		// needed
		let layout_with_qualification =
			self.qualify_duplicates_only(layout);

		for (i, column_layout) in
			layout_with_qualification.columns.iter().enumerate()
		{
			if i < self.len() {
				let column = &mut self[i];
				let data = std::mem::replace(
					column.data_mut(),
					ColumnData::undefined(0),
				);

				*column = match (&column_layout.schema, &column_layout.source) {
                    (Some(schema), Some(source)) => Column::FullyQualified(FullyQualified {
                        schema: schema.clone(),
                        source: source.clone(),
                        name: column_layout.name.clone(),
                        data,
                    }),
                    (None, Some(source)) => Column::SourceQualified(SourceQualified {
                        source: source.clone(),
                        name: column_layout.name.clone(),
                        data,
                    }),
                    (None, None) => match column {
                        Column::Unqualified(_) => Column::Unqualified(Unqualified {
                            name: column_layout.name.clone(),
                            data,
                        }),
                        _ => Column::ColumnQualified(ColumnQualified {
                            name: column_layout.name.clone(),
                            data,
                        }),
                    },
                    (Some(_), None) => Column::ColumnQualified(ColumnQualified {
                        name: column_layout.name.clone(),
                        data,
                    }),
                };
			}
		}
	}

	fn qualify_duplicates_only(
		&self,
		layout: &ColumnsLayout,
	) -> ColumnsLayout {
		// Group columns by name and check for ambiguity across
		// different table/schema contexts
		let mut name_groups: HashMap<
			String,
			Vec<(Option<String>, Option<String>)>,
		> = HashMap::new();
		for column_layout in &layout.columns {
			name_groups
				.entry(column_layout.name.clone())
				.or_insert_with(Vec::new)
				.push((
					column_layout.schema.clone(),
					column_layout.source.clone(),
				));
		}

		// Only qualify columns that appear more than once across
		// different table/schema contexts
		let qualified_columns: Vec<_> = layout
            .columns
            .iter()
            .map(|column_layout| {
                let contexts = name_groups.get(&column_layout.name).unwrap();

                // Check if this column name appears in different source/schema contexts
                let mut unique_contexts = HashSet::new();
                for (schema, source) in contexts {
                    unique_contexts.insert((schema.clone(), source.clone()));
                }

                // Qualify if there are duplicates OR if the layout explicitly specifies schema/source
                let has_duplicates = unique_contexts.len() > 1;
                let has_explicit_qualification =
                    column_layout.schema.is_some() || column_layout.source.is_some();

                if has_duplicates || has_explicit_qualification {
                    // This column has naming conflicts - add qualification using available source info
                    match (&column_layout.schema, &column_layout.source) {
                        (Some(schema), Some(source)) => ColumnLayout {
                            schema: Some(schema.clone()),
                            source: Some(source.clone()),
                            name: column_layout.name.clone(),
                        },
                        (None, Some(source)) => ColumnLayout {
                            schema: None,
                            source: Some(source.clone()),
                            name: column_layout.name.clone(),
                        },
                        _ => {
                            // No source info in layout, try to get it from existing columns
                            if let Some(existing_column) =
                                self.iter().find(|c| c.name() == column_layout.name)
                            {
                                match (existing_column.schema(), existing_column.source()) {
                                    (Some(schema), Some(source)) => ColumnLayout {
                                        schema: Some(schema.to_string()),
                                        source: Some(source.to_string()),
                                        name: column_layout.name.clone(),
                                    },
                                    (None, Some(source)) => ColumnLayout {
                                        schema: None,
                                        source: Some(source.to_string()),
                                        name: column_layout.name.clone(),
                                    },
                                    _ => {
                                        // Use columns name as fallback source qualification
                                        ColumnLayout {
                                            schema: None,
                                            source: None,
                                            name: column_layout.name.clone(),
                                        }
                                    }
                                }
                            } else {
                                // Use columns name as fallback source qualification
                                ColumnLayout {
                                    schema: None,
                                    source: None,
                                    name: column_layout.name.clone(),
                                }
                            }
                        }
                    }
                } else {
                    // No duplicates - remove unnecessary qualification
                    ColumnLayout { schema: None, source: None, name: column_layout.name.clone() }
                }
            })
            .collect();

		ColumnsLayout {
			columns: qualified_columns,
		}
	}
}
