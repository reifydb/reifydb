use super::index::{Index, IndexKey};
use crate::row::EncodedRow;
use crate::row::key::EncodedKey;
use indexmap::IndexMap;
use std::collections::HashMap;

pub struct StateStore {
    pub rows: IndexMap<usize, EncodedRow>,
    indices: HashMap<String, Index>,
    next_row_id: usize,
}

impl StateStore {
    pub fn new() -> Self {
        Self {
            rows: IndexMap::new(),
            indices: HashMap::new(),
            next_row_id: 0,
        }
    }
    
    pub fn create_index(&mut self, name: String, columns: Vec<usize>) -> crate::Result<()> {
        let mut index = Index::new(columns);
        
        // Populate index with existing data
        for (&row_id, row) in &self.rows {
            index.insert(row, row_id)?;
        }
        
        self.indices.insert(name, index);
        Ok(())
    }
    
    pub fn insert(&mut self, row: EncodedRow) -> crate::Result<usize> {
        let row_id = self.next_row_id;
        self.next_row_id += 1;
        
        // Update indices
        for index in self.indices.values_mut() {
            index.insert(&row, row_id)?;
        }
        
        self.rows.insert(row_id, row);
        Ok(row_id)
    }
    
    pub fn delete(&mut self, row_id: usize) -> crate::Result<Option<EncodedRow>> {
        if let Some(row) = self.rows.shift_remove(&row_id) {
            // Update indices
            for index in self.indices.values_mut() {
                index.remove(&row, row_id)?;
            }
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
    
    pub fn update(&mut self, row_id: usize, new_row: EncodedRow) -> crate::Result<Option<EncodedRow>> {
        if let Some(old_row) = self.rows.get(&row_id) {
            let old_row_clone = old_row.clone();
            
            // Update indices
            for index in self.indices.values_mut() {
                index.remove(&old_row_clone, row_id)?;
                index.insert(&new_row, row_id)?;
            }
            
            self.rows.insert(row_id, new_row);
            Ok(Some(old_row_clone))
        } else {
            Ok(None)
        }
    }
    
    pub fn get(&self, row_id: usize) -> Option<&EncodedRow> {
        self.rows.get(&row_id)
    }
    
    pub fn lookup_by_index(&self, index_name: &str, key: &IndexKey) -> Vec<&EncodedRow> {
        if let Some(index) = self.indices.get(index_name) {
            if let Some(row_ids) = index.lookup(key) {
                return row_ids.iter()
                    .filter_map(|&row_id| self.rows.get(&row_id))
                    .collect();
            }
        }
        Vec::new()
    }
    
    pub fn all_rows(&self) -> impl Iterator<Item = &EncodedRow> {
        self.rows.values()
    }
    
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    pub fn clear(&mut self) {
        self.rows.clear();
        self.indices.clear();
        self.next_row_id = 0;
    }
    
    pub fn get_index(&self, name: &str) -> Option<&Index> {
        self.indices.get(name)
    }
    
    pub fn has_index(&self, name: &str) -> bool {
        self.indices.contains_key(name)
    }
}