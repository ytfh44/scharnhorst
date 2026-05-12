use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{SchemaError, SchemaResult};
use crate::field_semantic::FieldSemantic;

/// Specification for a single column within a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub semantic: FieldSemantic,
 /// Storage type hint (e.g. "i64", "f64", "utf8", "bool").
    pub storage_type: String,
    pub nullable: bool,
}

impl ColumnSpec {
    pub fn new(name: impl Into<String>, semantic: FieldSemantic, storage_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            semantic,
            storage_type: storage_type.into(),
            nullable: false,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

/// Specification for a table (entity archetype) in the simulation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableSpec {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
 /// Map from column name -> index in `columns` for fast lookup.
 /// Skipped during serialization, rebuilt on deserialization.
    #[serde(skip)]
    pub column_index: HashMap<String, usize>,
}

impl<'de> serde::Deserialize<'de> for TableSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            name: String,
            columns: Vec<ColumnSpec>,
        }
        let helper = Helper::deserialize(deserializer)?;
        let mut spec = TableSpec {
            name: helper.name,
            columns: helper.columns,
            column_index: HashMap::new(),
        };
        spec.rebuild_index();
        Ok(spec)
    }
}

impl TableSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            column_index: HashMap::new(),
        }
    }

    pub fn with_column(mut self, col: ColumnSpec) -> SchemaResult<Self> {
        if self.column_index.contains_key(&col.name) {
            return Err(SchemaError::DuplicateColumn(col.name));
        }
        let idx = self.columns.len();
        self.columns.push(col.clone());
        self.column_index.insert(col.name, idx);
        Ok(self)
    }

    pub fn column_by_name(&self, name: &str) -> Option<&ColumnSpec> {
        self.column_index
            .get(name)
            .and_then(|&idx| self.columns.get(idx))
    }

    pub fn column_index_of(&self, name: &str) -> Option<usize> {
        self.column_index.get(name).copied()
    }

    pub fn primary_key_column(&self) -> Option<&ColumnSpec> {
        self.columns.iter().find(|c| matches!(c.semantic, FieldSemantic::Id))
    }

    pub fn foreign_key_columns(&self) -> impl Iterator<Item = &ColumnSpec> {
        self.columns.iter().filter(|c| c.semantic.is_reference())
    }

 /// Rebuild the column index from the columns vector.
 /// Called after deserialization since `column_index` is `#[serde(skip)]`.
    pub fn rebuild_index(&mut self) {
        self.column_index = self.columns.iter().enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_id_col() -> ColumnSpec {
        ColumnSpec::new("id", FieldSemantic::Id, "u64")
    }

    fn make_name_col() -> ColumnSpec {
        ColumnSpec::new("name", FieldSemantic::Name, "utf8")
    }

    fn make_fk_col(target: &str) -> ColumnSpec {
        ColumnSpec::new(
            "fk",
            FieldSemantic::ForeignKey {
                target_table: target.to_string(),
            },
            "u64",
        )
    }

 // ---- ColumnSpec ----

    #[test]
    fn column_spec_construction() {
        let col = ColumnSpec::new("health", FieldSemantic::Quantity, "i64");
        assert_eq!(col.name, "health");
        assert_eq!(col.semantic, FieldSemantic::Quantity);
        assert_eq!(col.storage_type, "i64");
        assert!(!col.nullable);
    }

    #[test]
    fn column_spec_with_nullable_true() {
        let col = ColumnSpec::new("age", FieldSemantic::Quantity, "i64").with_nullable(true);
        assert!(col.nullable);
    }

    #[test]
    fn column_spec_with_nullable_false() {
        let col = ColumnSpec::new("name", FieldSemantic::Name, "utf8").with_nullable(false);
        assert!(!col.nullable);
    }

 // ---- TableSpec::with_column ----

    #[test]
    fn table_spec_with_column() -> SchemaResult<()> {
        let spec = TableSpec::new("Unit")
            .with_column(make_id_col())?
            .with_column(make_name_col())?;
        assert_eq!(spec.name, "Unit");
        assert_eq!(spec.columns.len(), 2);
        Ok(())
    }

    #[test]
    fn duplicate_column_detection() {
        let col = make_id_col();
        let result = TableSpec::new("Unit")
            .with_column(col.clone())
            .and_then(|t| t.with_column(col));
        assert_eq!(result, Err(SchemaError::DuplicateColumn("id".to_string())));
    }

 // ---- column_by_name ----

    #[test]
    fn column_by_name_found() -> SchemaResult<()> {
        let spec = TableSpec::new("Unit")
            .with_column(make_id_col())?
            .with_column(make_name_col())?;
        let col = spec.column_by_name("name").unwrap();
        assert_eq!(col.name, "name");
        assert_eq!(col.semantic, FieldSemantic::Name);
        Ok(())
    }

    #[test]
    fn column_by_name_not_found() {
        let spec = TableSpec::new("Unit");
        assert!(spec.column_by_name("nonexistent").is_none());
    }

 // ---- column_index_of ----

    #[test]
    fn column_index_of_found() -> SchemaResult<()> {
        let spec = TableSpec::new("Unit")
            .with_column(make_id_col())?
            .with_column(make_name_col())?;
        assert_eq!(spec.column_index_of("id"), Some(0));
        assert_eq!(spec.column_index_of("name"), Some(1));
        Ok(())
    }

    #[test]
    fn column_index_of_not_found() {
        let spec = TableSpec::new("Unit");
        assert_eq!(spec.column_index_of("nope"), None);
    }

 // ---- primary_key_column ----

    #[test]
    fn primary_key_column_found() -> SchemaResult<()> {
        let spec = TableSpec::new("Unit")
            .with_column(make_name_col())?
            .with_column(make_id_col())?;
        let pk = spec.primary_key_column().unwrap();
        assert_eq!(pk.name, "id");
        assert_eq!(pk.semantic, FieldSemantic::Id);
        Ok(())
    }

    #[test]
    fn primary_key_column_not_found() {
        let spec = TableSpec::new("Unit");
        assert!(spec.primary_key_column().is_none());
    }

 // ---- foreign_key_columns ----

    #[test]
    fn foreign_key_columns_present() -> SchemaResult<()> {
        let spec = TableSpec::new("Unit")
            .with_column(make_id_col())?
            .with_column(make_fk_col("Other"))?;
        let fks: Vec<&ColumnSpec> = spec.foreign_key_columns().collect();
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].name, "fk");
        Ok(())
    }

    #[test]
    fn foreign_key_columns_empty() -> SchemaResult<()> {
        let spec = TableSpec::new("Unit")
            .with_column(make_id_col())?
            .with_column(make_name_col())?;
        let fks: Vec<&ColumnSpec> = spec.foreign_key_columns().collect();
        assert!(fks.is_empty());
        Ok(())
    }

 // ---- Serialize/Deserialize ----

    #[test]
    fn table_spec_serialize_roundtrip() -> SchemaResult<()> {
        let spec = TableSpec::new("Unit")
            .with_column(make_id_col())?
            .with_column(make_name_col())?;
        let json = serde_json::to_string(&spec).unwrap();
        let back: TableSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "Unit");
 // Note: column_index is serde(skip), so after deserialization it will be empty
        Ok(())
    }
}
