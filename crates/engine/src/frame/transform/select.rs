// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::Frame;

impl Frame {
    pub fn select(&mut self, names: &[&str]) -> crate::frame::Result<()> {
        let mut selected: Vec<usize> =
            names.into_iter().filter_map(|&name| self.index.get(name).cloned()).collect();

        if selected.is_empty() {
            self.columns = vec![];
            return Ok(());
        }

        selected.sort();
        selected.reverse();

        let mut columns = Vec::with_capacity(selected.len());
        for idx in selected {
            columns.push(self.columns.remove(idx));
        }

        columns.reverse();
        self.columns = columns;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::{FrameColumn, Frame};

    #[test]
    fn test_select_subset_of_columns() {
        let mut test_instance = make_test_instance();
        test_instance.select(&["name", "score"]).unwrap();

        assert_eq!(test_instance.columns.len(), 2);
        assert_eq!(test_instance.columns[0].name, "name");
        assert_eq!(test_instance.columns[1].name, "score");
    }

    #[test]
    fn test_select_single_column() {
        let mut test_instance = make_test_instance();
        test_instance.select(&["id"]).unwrap();

        assert_eq!(test_instance.columns.len(), 1);
        assert_eq!(test_instance.columns[0].name, "id");
    }

    #[test]
    fn test_select_all_columns() {
        let mut test_instance = make_test_instance();
        test_instance.select(&["id", "name", "score"]).unwrap();

        assert_eq!(test_instance.columns.len(), 3);
        assert_eq!(test_instance.columns[0].name, "id");
        assert_eq!(test_instance.columns[1].name, "name");
        assert_eq!(test_instance.columns[2].name, "score");
    }

    #[test]
    fn test_select_no_columns() {
        let mut test_instance = make_test_instance();
        test_instance.select(&[]).unwrap();

        assert_eq!(test_instance.columns.len(), 0);
    }

    #[test]
    fn test_select_non_existent() {
        let mut test_instance = make_test_instance();
        test_instance.select(&["nonexistent"]).unwrap();

        assert_eq!(test_instance.columns.len(), 0);
    }

    #[test]
    fn test_select_with_missing_column_names() {
        let mut test_instance = make_test_instance();
        test_instance.select(&["id", "nonexistent", "score"]).unwrap();

        assert_eq!(test_instance.columns.len(), 2);
        assert_eq!(test_instance.columns[0].name, "id");
        assert_eq!(test_instance.columns[1].name, "score");
    }

    fn make_test_instance() -> Frame {
        Frame::new(vec![
			FrameColumn::int2("id", [1, 2]),
			FrameColumn::utf8("name", ["Alice", "Bob"]),
			FrameColumn::int2("score", [23, 32]),
        ])
    }
}
