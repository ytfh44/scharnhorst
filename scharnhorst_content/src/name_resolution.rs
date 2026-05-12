use std::collections::HashMap;

use scharnhorst_core::RowId;

use crate::error::{ContentError, ContentResult};

/// Maps human-readable string names to stable integer IDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NameResolver {
    name_to_id: HashMap<String, RowId>,
    id_to_name: HashMap<RowId, String>,
    next_id: u64,
}

impl NameResolver {
    pub fn new() -> Self {
        Self::default()
    }

 /// Register a name, assigning a new stable ID.
 /// Returns an error if the name already exists.
    pub fn register(&mut self, name: impl Into<String>) -> ContentResult<RowId> {
        let name = name.into();
        if self.name_to_id.contains_key(&name) {
            return Err(ContentError::DuplicateName(name));
        }
        let id = RowId::new(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        self.name_to_id.insert(name.clone(), id);
        self.id_to_name.insert(id, name);
        Ok(id)
    }

 /// Lookup the ID for a name.
    pub fn resolve(&self, name: &str) -> ContentResult<RowId> {
        self.name_to_id
            .get(name)
            .copied()
            .ok_or_else(|| ContentError::NameResolutionFailed(name.to_owned()))
    }

 /// Reverse lookup: get the name for an ID.
    pub fn name_of(&self, id: RowId) -> ContentResult<String> {
        self.id_to_name
            .get(&id)
            .cloned()
            .ok_or_else(|| ContentError::NameResolutionFailed(format!("{}", id.0)))
    }

 /// Returns true if the name is known.
    pub fn contains_name(&self, name: &str) -> bool {
        self.name_to_id.contains_key(name)
    }

 /// Returns true if the ID is known.
    pub fn contains_id(&self, id: RowId) -> bool {
        self.id_to_name.contains_key(&id)
    }

 /// Resolve a name to its ID, or auto-assign a new ID if unknown.
    pub fn resolve_or_assign(&mut self, name: impl Into<String>) -> RowId {
        let name = name.into();
        if let Some(&id) = self.name_to_id.get(&name) {
            return id;
        }
        let id = RowId::new(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        self.name_to_id.insert(name.clone(), id);
        self.id_to_name.insert(id, name);
        id
    }

 /// Iterate over all registered names.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.name_to_id.keys().map(|s| s.as_str())
    }

 /// Iterate over all registered IDs.
    pub fn ids(&self) -> impl Iterator<Item = RowId> + '_ {
        self.id_to_name.keys().copied()
    }

 /// Number of registered entries.
    pub fn len(&self) -> usize {
        self.name_to_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.name_to_id.is_empty()
    }
}

/// A namespaced collection of resolvers, one per table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NamespaceResolver {
    tables: HashMap<String, NameResolver>,
    next_id: u64,
}

impl NamespaceResolver {
    pub fn new() -> Self {
        Self::default()
    }

 /// Ensure a namespace exists, returning a mutable reference.
    pub fn namespace(&mut self, table: impl Into<String>) -> &mut NameResolver {
        let resolver = self.tables.entry(table.into()).or_default();
        resolver.next_id = self.next_id;
        resolver
    }

 /// Bump the shared next_id after registering in a namespace.
    pub fn sync_next_id(&mut self) {
        self.next_id = self.tables.values().map(|r| r.next_id).max().unwrap_or(0);
    }

 /// Resolve a name within a specific table namespace.
    pub fn resolve(&self, table: &str, name: &str) -> ContentResult<RowId> {
        self.tables
            .get(table)
            .ok_or_else(|| ContentError::NameResolutionFailed(format!("table: {}", table)))?
            .resolve(name)
    }

 /// Resolve a name to its ID within a namespace, or auto-assign if unknown.
    pub fn resolve_or_assign(&mut self, table: &str, name: &str) -> RowId {
        let resolver = self.tables.entry(table.to_owned()).or_default();
        resolver.next_id = self.next_id;
        let id = resolver.resolve_or_assign(name);
        self.next_id = resolver.next_id;
        id
    }

 /// Reverse lookup within a table namespace.
    pub fn name_of(&self, table: &str, id: RowId) -> ContentResult<String> {
        self.tables
            .get(table)
            .ok_or_else(|| ContentError::NameResolutionFailed(format!("table: {}", table)))?
            .name_of(id)
    }

 /// Returns all known table names.
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_or_assign_new_name() {
        let mut resolver = NameResolver::new();
        let id = resolver.resolve_or_assign("FRA");
        assert_eq!(resolver.resolve("FRA").unwrap(), id);
        assert_eq!(resolver.name_of(id).unwrap(), "FRA");
    }

    #[test]
    fn resolve_or_assign_existing_name() {
        let mut resolver = NameResolver::new();
        let id1 = resolver.register("FRA").unwrap();
        let id2 = resolver.resolve_or_assign("FRA");
        assert_eq!(id1, id2);
    }

    #[test]
    fn resolve_or_assign_idempotent() {
        let mut resolver = NameResolver::new();
        let id1 = resolver.resolve_or_assign("ENG");
        let id2 = resolver.resolve_or_assign("ENG");
        assert_eq!(id1, id2);
        assert_eq!(resolver.len(), 1);
    }

    #[test]
    fn resolve_or_assign_sequential_ids() {
        let mut resolver = NameResolver::new();
        let id_a = resolver.resolve_or_assign("A");
        let id_b = resolver.resolve_or_assign("B");
        assert!(id_a.0 < id_b.0);
        assert_eq!(resolver.len(), 2);
    }

    #[test]
    fn namespace_resolve_or_assign() {
        let mut ns = NamespaceResolver::new();
        let id1 = ns.resolve_or_assign("actors", "FRA");
        let id2 = ns.resolve_or_assign("actors", "FRA");
        assert_eq!(id1, id2);
        assert_eq!(ns.resolve("actors", "FRA").unwrap(), id1);
    }

    #[test]
    fn namespace_resolve_or_assign_cross_table() {
        let mut ns = NamespaceResolver::new();
        let actor_id = ns.resolve_or_assign("actors", "FRA");
        let province_id = ns.resolve_or_assign("provinces", "Paris");
        assert_ne!(actor_id, province_id);
    }

    #[test]
    fn name_resolver_len_and_is_empty() {
        let mut resolver = NameResolver::new();
        assert!(resolver.is_empty());
        assert_eq!(resolver.len(), 0);
        resolver.register("A").unwrap();
        assert!(!resolver.is_empty());
        assert_eq!(resolver.len(), 1);
    }

    #[test]
    fn contains_name_and_id() {
        let mut resolver = NameResolver::new();
        let id = resolver.register("X").unwrap();
        assert!(resolver.contains_name("X"));
        assert!(!resolver.contains_name("Y"));
        assert!(resolver.contains_id(id));
        assert!(!resolver.contains_id(RowId::new(999)));
    }

    #[test]
    fn names_and_ids_iterators() {
        let mut resolver = NameResolver::new();
        resolver.register("A").unwrap();
        resolver.register("B").unwrap();
        let names: Vec<&str> = resolver.names().collect();
        assert_eq!(names.len(), 2);
        let ids: Vec<RowId> = resolver.ids().collect();
        assert_eq!(ids.len(), 2);
    }
}
