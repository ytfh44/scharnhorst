use std::collections::HashMap;

use arrow_array::RecordBatch;
use scharnhorst_arrow_store::{ArrowStore, MutationMode};
use scharnhorst_core::Tick;
use scharnhorst_schema::{SchemaRegistry, TableSpec};

use crate::error::{ContentError, ContentResult};
use crate::name_resolution::NamespaceResolver;
use crate::overlay::OverlayResolver;

/// Raw content definition before compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTableDef {
    pub spec: TableSpec,
 /// Rows as columnar string data, keyed by column name.
    pub rows: HashMap<String, Vec<String>>,
}

/// Input to the content compilation pipeline.
#[derive(Debug, Clone, Default)]
pub struct CompilationInput {
    pub tables: Vec<RawTableDef>,
    pub overlays: OverlayResolver,
    pub resolver: NamespaceResolver,
}

impl CompilationInput {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_table(mut self, def: RawTableDef) -> Self {
        self.tables.push(def);
        self
    }

    pub fn with_overlays(mut self, overlays: OverlayResolver) -> Self {
        self.overlays = overlays;
        self
    }

    pub fn with_resolver(mut self, resolver: NamespaceResolver) -> Self {
        self.resolver = resolver;
        self
    }
}

/// Output produced by content compilation.
#[derive(Debug, Clone)]
pub struct CompilationOutput {
 /// The populated Arrow store with compiled tables.
    pub store: ArrowStore,
 /// The populated schema registry with all TableSpecs.
    pub registry: SchemaRegistry,
 /// Final namespace resolver after name registration.
    pub resolver: NamespaceResolver,
}

/// Compiles raw content definitions into binary Arrow tables.
pub struct ContentCompiler;

impl ContentCompiler {
    pub fn new() -> Self {
        Self
    }

 /// Compile all raw definitions into the Arrow store and schema registry.
    pub fn compile(&self, input: CompilationInput) -> ContentResult<CompilationOutput> {
        let mut store = ArrowStore::new();
        let mut registry = SchemaRegistry::new();
        let resolver = input.resolver;

        for def in &input.tables {
            Self::compile_table(&mut store, &mut registry, def)?;
        }

        Ok(CompilationOutput {
            store,
            registry,
            resolver,
        })
    }

    fn compile_table(
        store: &mut ArrowStore,
        registry: &mut SchemaRegistry,
        def: &RawTableDef,
    ) -> ContentResult<()> {
        registry.register(def.spec.clone()).map_err(|e| {
            ContentError::CompilationFailed {
                table: def.spec.name.clone(),
                reason: format!("registry error: {}", e),
            }
        })?;

        store
            .create_table(&def.spec, MutationMode::AppendOnly)
            .map_err(|e| ContentError::CompilationFailed {
                table: def.spec.name.clone(),
                reason: format!("store error: {}", e),
            })?;

        let batches = Self::build_batches(def)?;
        store
            .append_batches(&def.spec.name, Tick::ZERO, batches)
            .map_err(|e| ContentError::CompilationFailed {
                table: def.spec.name.clone(),
                reason: format!("batch append error: {}", e),
            })?;

        Ok(())
    }

    fn build_batches(def: &RawTableDef) -> ContentResult<Vec<RecordBatch>> {
        if def.rows.is_empty() {
            return Ok(Vec::new());
        }

        let batch = build_record_batch_from_strings(&def.spec, &def.rows).map_err(|e| {
            ContentError::CompilationFailed {
                table: def.spec.name.clone(),
                reason: e.to_string(),
            }
        })?;

        Ok(vec![batch])
    }
}

impl Default for ContentCompiler {
    fn default() -> Self {
        Self::new()
    }
}

fn build_record_batch_from_strings(
    spec: &TableSpec,
    rows: &HashMap<String, Vec<String>>,
) -> Result<RecordBatch, ContentError> {
    use arrow_array::{ArrayRef, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let fields: Vec<Field> = spec
        .columns
        .iter()
        .map(|c| Field::new(&c.name, DataType::Utf8, c.nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<ArrayRef> = spec
        .columns
        .iter()
        .map(|col| -> Result<ArrayRef, ContentError> {
            let values = rows.get(&col.name).ok_or_else(|| {
                ContentError::CompilationFailed {
                    table: spec.name.clone(),
                    reason: format!("missing column data: {}", col.name),
                }
            })?;
            let arr: ArrayRef = Arc::new(StringArray::from(values.clone()));
            Ok(arr)
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, arrays).map_err(|e| ContentError::CompilationFailed {
        table: spec.name.clone(),
        reason: format!("arrow batch error: {}", e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic};

    fn make_spec(name: &str) -> TableSpec {
        TableSpec::new(name)
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .unwrap()
            .with_column(ColumnSpec::new("label", FieldSemantic::Name, "utf8"))
            .unwrap()
    }

    #[test]
    fn compile_empty_tables() {
        let input = CompilationInput::new();
        let compiler = ContentCompiler::new();
        let output = compiler.compile(input).unwrap();
        assert_eq!(output.store.table_count().unwrap(), 0);
    }

    #[test]
    fn compile_single_table_with_data() {
        let spec = make_spec("items");
        let mut rows: HashMap<String, Vec<String>> = HashMap::new();
        rows.insert("id".to_owned(), vec!["1".to_owned(), "2".to_owned()]);
        rows.insert(
            "label".to_owned(),
            vec!["alpha".to_owned(), "beta".to_owned()],
        );

        let input = CompilationInput::new().with_table(RawTableDef { spec, rows });
        let output = ContentCompiler::new().compile(input).unwrap();
        assert_eq!(output.store.table_count().unwrap(), 1);
        assert_eq!(output.registry.table_count(), 1);
        assert!(output.registry.contains("items"));
    }

    #[test]
    fn build_batches_empty_rows() {
        let spec = make_spec("empty_table");
        let rows: HashMap<String, Vec<String>> = HashMap::new();
        let def = RawTableDef { spec, rows };
        let batches = ContentCompiler::build_batches(&def).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn content_compiler_default() {
        let _compiler = ContentCompiler;
    }

    #[test]
    fn compilation_input_builder() {
        let input = CompilationInput::new()
            .with_table(RawTableDef {
                spec: make_spec("t1"),
                rows: HashMap::new(),
            })
            .with_overlays(OverlayResolver::new())
            .with_resolver(NamespaceResolver::new());
        assert_eq!(input.tables.len(), 1);
    }

    #[test]
    fn raw_table_def_equality() {
        let spec = make_spec("eq");
        let a = RawTableDef {
            spec: spec.clone(),
            rows: HashMap::new(),
        };
        let b = RawTableDef {
            spec,
            rows: HashMap::new(),
        };
        assert_eq!(a, b);
    }
}
