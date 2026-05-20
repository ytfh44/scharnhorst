use bevy::prelude::Component;
use scharnhorst_core::RowId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Component)]
pub struct ViewOf {
    row_id: RowId,
    table_name: String,
    generation: u64,
}

impl ViewOf {
    pub fn new(row_id: RowId, table_name: impl Into<String>) -> Self {
        Self {
            row_id,
            table_name: table_name.into(),
            generation: 0,
        }
    }

    pub fn with_generation(row_id: RowId, table_name: impl Into<String>, generation: u64) -> Self {
        Self {
            row_id,
            table_name: table_name.into(),
            generation,
        }
    }

    pub const fn row_id(&self) -> RowId {
        self.row_id
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
    }

    pub fn with_updated_generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_of_stores_row_id_and_table_name() {
        let view = ViewOf::new(RowId::new(42), "provinces");
        assert_eq!(view.row_id(), RowId::new(42));
        assert_eq!(view.table_name(), "provinces");
    }

    #[test]
    fn view_of_is_clone() {
        let a = ViewOf::new(RowId::new(1), "units");
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn view_of_default_generation_is_zero() {
        let view = ViewOf::new(RowId::new(1), "units");
        assert_eq!(view.generation(), 0);
    }

    #[test]
    fn view_of_with_generation() {
        let view = ViewOf::with_generation(RowId::new(1), "units", 7);
        assert_eq!(view.generation(), 7);
    }

    #[test]
    fn view_of_set_generation() {
        let mut view = ViewOf::new(RowId::new(1), "units");
        view.set_generation(5);
        assert_eq!(view.generation(), 5);
    }

    #[test]
    fn view_of_with_updated_generation() {
        let view = ViewOf::new(RowId::new(1), "units");
        let updated = view.with_updated_generation(3);
        assert_eq!(updated.generation(), 3);
        assert_eq!(updated.row_id(), RowId::new(1));
        assert_eq!(updated.table_name(), "units");
    }
}
