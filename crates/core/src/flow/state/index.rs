use crate::Value;
use crate::flow::row::Row;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey(pub Vec<Value>);

impl IndexKey {
    pub fn from_row(row: &Row, columns: &[usize]) -> crate::Result<Self> {
        let mut values = Vec::new();
        for &idx in columns {
            values.push(row.get(idx)?);
        }
        Ok(Self(values))
    }

    pub fn from_values(values: Vec<Value>) -> Self {
        Self(values)
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

    pub fn get_by_values(&self, values: Vec<Value>) -> Option<&HashSet<usize>> {
        let key = IndexKey::from_values(values);
        self.lookup(&key)
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }
}
