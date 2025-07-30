use crate::flow::row::Row;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use reifydb_core::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey(pub Vec<Value>);

impl IndexKey {
    pub fn from_row(row: &Row, columns: &[usize]) -> crate::Result<Self> {
        let mut data = Vec::new();
        for &idx in columns {
            data.push(row.get(idx)?);
        }
        Ok(Self(data))
    }

    pub fn from_data(data: Vec<Value>) -> Self {
        Self(data)
    }
}

pub struct Index {
    columns: Vec<usize>,
    data: HashMap<IndexKey, HashSet<usize>>,
}

impl Index {
    pub fn new(columns: Vec<usize>) -> Self {
        Self { columns, data: HashMap::new() }
    }

    pub fn insert(&mut self, row: &Row, row_idx: usize) -> crate::Result<()> {
        let key = IndexKey::from_row(row, &self.columns)?;
        self.data.entry(key).or_insert_with(HashSet::new).insert(row_idx);
        Ok(())
    }

    pub fn remove(&mut self, row: &Row, row_idx: usize) -> crate::Result<()> {
        let key = IndexKey::from_row(row, &self.columns)?;
        if let Some(indices) = self.data.get_mut(&key) {
            indices.remove(&row_idx);
            if indices.is_empty() {
                self.data.remove(&key);
            }
        }
        Ok(())
    }

    pub fn lookup(&self, key: &IndexKey) -> Option<&HashSet<usize>> {
        self.data.get(key)
    }

    pub fn get_by_data(&self, data: Vec<Value>) -> Option<&HashSet<usize>> {
        let key = IndexKey::from_data(data);
        self.lookup(&key)
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }
}
