// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::{HashMap, HashSet};

use reifydb_type::Fragment;

use super::{ColumnLayout, ColumnsLayout};
use crate::value::column::{Column, ColumnComputed, ColumnData, Columns, SourceQualified};

impl<'a> Columns<'a> {
	pub fn apply_layout(&mut self, layout: &ColumnsLayout<'a>) {
		// Check for duplicate column names and qualify them only when
		// needed
		let layout_with_qualification = self.qualify_duplicates_only(layout);

		for (i, column_layout) in layout_with_qualification.columns.iter().enumerate() {
			if i < self.len() {
				let column = &mut self[i];
				let data = std::mem::replace(column.data_mut(), ColumnData::undefined(0));

				*column = match (&column_layout.namespace, &column_layout.source) {
					(Some(_namespace), Some(source)) => Column::SourceQualified(SourceQualified {
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
						_ => Column::Computed(ColumnComputed {
							name: column_layout.name.clone(),
							data,
						}),
					},
					(Some(_), None) => Column::Computed(ColumnComputed {
						name: column_layout.name.clone(),
						data,
					}),
				};
			}
		}
	}

	fn qualify_duplicates_only(&self, layout: &ColumnsLayout<'a>) -> ColumnsLayout<'a> {
		// Group columns by name and check for ambiguity across
		// different table/namespace contexts
		let mut name_groups: HashMap<String, Vec<(Option<String>, Option<String>)>> = HashMap::new();
		for column_layout in &layout.columns {
			name_groups.entry(column_layout.name.text().to_string()).or_insert_with(Vec::new).push((
				column_layout.namespace.as_ref().map(|f| f.text().to_string()),
				column_layout.source.as_ref().map(|f| f.text().to_string()),
			));
		}

		// Only qualify columns that appear more than once across
		// different table/namespace contexts
		let qualified_columns: Vec<_> = layout
			.columns
			.iter()
			.map(|column_layout| {
				let contexts = name_groups.get(column_layout.name.text()).unwrap();

				// Check if this column name appears in different source/namespace contexts
				let mut unique_contexts = HashSet::new();
				for (namespace, source) in contexts {
					unique_contexts.insert((namespace.clone(), source.clone()));
				}

				// Qualify if there are duplicates OR if the layout explicitly specifies
				// namespace/source
				let has_duplicates = unique_contexts.len() > 1;
				let has_explicit_qualification =
					column_layout.namespace.is_some() || column_layout.source.is_some();

				if has_duplicates || has_explicit_qualification {
					// This column has naming conflicts - add qualification using available source
					// info
					match (&column_layout.namespace, &column_layout.source) {
						(Some(namespace), Some(source)) => ColumnLayout {
							namespace: Some(namespace.clone()),
							source: Some(source.clone()),
							name: column_layout.name.clone(),
						},
						(None, Some(source)) => ColumnLayout {
							namespace: None,
							source: Some(source.clone()),
							name: column_layout.name.clone(),
						},
						_ => {
							// No source info in layout, try to get it from existing columns
							if let Some(existing_column) =
								self.iter().find(|c| c.name() == &column_layout.name)
							{
								match (
									existing_column.namespace(),
									existing_column.source(),
								) {
									(Some(namespace), Some(source)) => {
										ColumnLayout {
											namespace: Some(
												Fragment::owned_internal(namespace.text())
											),
											source: Some(Fragment::owned_internal(source.text())),
											name: column_layout
												.name
												.clone(),
										}
									}
									(None, Some(source)) => ColumnLayout {
										namespace: None,
										source: Some(Fragment::owned_internal(source.text())),
										name: column_layout.name.clone(),
									},
									_ => {
										// Use columns name as fallback source
										// qualification
										ColumnLayout {
											namespace: None,
											source: None,
											name: column_layout
												.name
												.clone(),
										}
									}
								}
							} else {
								// Use columns name as fallback source qualification
								ColumnLayout {
									namespace: None,
									source: None,
									name: column_layout.name.clone(),
								}
							}
						}
					}
				} else {
					// No duplicates - remove unnecessary qualification
					ColumnLayout {
						namespace: None,
						source: None,
						name: column_layout.name.clone(),
					}
				}
			})
			.collect();

		ColumnsLayout {
			columns: qualified_columns,
		}
	}
}
