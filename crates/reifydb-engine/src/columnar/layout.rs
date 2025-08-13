// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{
	Column, ColumnData, ColumnQualified, Columns, FullyQualified,
	TableQualified, Unqualified,
};

#[derive(Debug, Clone)]
pub struct ColumnsLayout {
	pub columns: Vec<ColumnLayout>,
}

impl ColumnsLayout {
	pub fn from_columns(columns: &Columns) -> Self {
		Self {
			columns: columns
				.iter()
				.map(|c| ColumnLayout::from_column(c))
				.collect(),
		}
	}
}

#[derive(Debug, Clone)]
pub struct ColumnLayout {
	pub schema: Option<String>,
	pub table: Option<String>,
	pub name: String,
}

impl ColumnLayout {
	pub fn from_column(column: &Column) -> Self {
		Self {
			schema: column.schema().map(|s| s.to_string()),
			table: column.table().map(|s| s.to_string()),
			name: column.name().to_string(),
		}
	}
}

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

				*column = match (&column_layout.schema, &column_layout.table) {
                    (Some(schema), Some(table)) => Column::FullyQualified(FullyQualified {
                        schema: schema.clone(),
                        table: table.clone(),
                        name: column_layout.name.clone(),
                        data,
                    }),
                    (None, Some(table)) => Column::TableQualified(TableQualified {
                        table: table.clone(),
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
		use std::collections::HashMap;

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
					column_layout.table.clone(),
				));
		}

		// Only qualify columns that appear more than once across
		// different table/schema contexts
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

                // Qualify if there are duplicates OR if the layout explicitly specifies schema/table
                let has_duplicates = unique_contexts.len() > 1;
                let has_explicit_qualification =
                    column_layout.schema.is_some() || column_layout.table.is_some();

                if has_duplicates || has_explicit_qualification {
                    // This column has naming conflicts - add qualification using available table info
                    match (&column_layout.schema, &column_layout.table) {
                        (Some(schema), Some(table)) => ColumnLayout {
                            schema: Some(schema.clone()),
                            table: Some(table.clone()),
                            name: column_layout.name.clone(),
                        },
                        (None, Some(table)) => ColumnLayout {
                            schema: None,
                            table: Some(table.clone()),
                            name: column_layout.name.clone(),
                        },
                        _ => {
                            // No table info in layout, try to get it from existing columns
                            if let Some(existing_column) =
                                self.iter().find(|c| c.name() == column_layout.name)
                            {
                                match (existing_column.schema(), existing_column.table()) {
                                    (Some(schema), Some(table)) => ColumnLayout {
                                        schema: Some(schema.to_string()),
                                        table: Some(table.to_string()),
                                        name: column_layout.name.clone(),
                                    },
                                    (None, Some(table)) => ColumnLayout {
                                        schema: None,
                                        table: Some(table.to_string()),
                                        name: column_layout.name.clone(),
                                    },
                                    _ => {
                                        // Use columns name as fallback table qualification
                                        ColumnLayout {
                                            schema: None,
                                            table: None,
                                            name: column_layout.name.clone(),
                                        }
                                    }
                                }
                            } else {
                                // Use columns name as fallback table qualification
                                ColumnLayout {
                                    schema: None,
                                    table: None,
                                    name: column_layout.name.clone(),
                                }
                            }
                        }
                    }
                } else {
                    // No duplicates - remove unnecessary qualification
                    ColumnLayout { schema: None, table: None, name: column_layout.name.clone() }
                }
            })
            .collect();

		ColumnsLayout {
			columns: qualified_columns,
		}
	}
}
