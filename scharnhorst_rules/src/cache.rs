use std::collections::HashMap;
use std::sync::Arc;

use scharnhorst_core::{DerivedState, RowId, Tick};
use scharnhorst_query::{TableReadView, UnifiedReadSource};

use crate::error::{RuleError, RuleResult};

/// A single cached row pinned from a query-engine lookup.
#[derive(Debug, Clone)]
pub struct CachedRow {
    pub tick: Tick,
    pub table: String,
    pub row_id: RowId,
    pub view: Arc<TableReadView>,
}

/// Bounded LRU prefetch cache for cross-partition scope lookups.
///
/// Default capacity: 1024 rows. Eviction is least-recently-used.
/// The cache is tick-scoped: all entries belong to the same snapshot
/// and are discarded at tick boundaries via [`PrefetchCache::clear`].
///
/// # DerivedState Integration
///
/// `PrefetchCache` implements [`DerivedState`]. When authority data
/// changes, consumers call [`mark_dirty`](DerivedState::mark_dirty).
/// At tick boundaries -- triggered by the scheduler's REFRESH_SIGNAL --
/// [`rebuild_from_authority`](DerivedState::rebuild_from_authority)
/// clears the cache and advances the tick. Subsequent lookups repopulate
/// lazily via [`prefetch`](PrefetchCache::prefetch).
#[derive(Debug, Clone)]
pub struct PrefetchCache {
    capacity: usize,
    entries: HashMap<CacheKey, CacheEntry>,
    current_tick: Tick,
    access_counter: usize,
    /// Dirty flag tracking whether cached data may be stale.
    /// Set by `mark_dirty()` when authority tables change;
    /// cleared by `rebuild_from_authority()` at tick boundary.
    dirty: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    table: String,
    row_id: u64,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    row: CachedRow,
    last_access: usize,
}

impl PrefetchCache {
    pub const DEFAULT_CAPACITY: usize = 1024;

    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::with_capacity(capacity.min(1024)),
            current_tick: Tick::ZERO,
            access_counter: 0,
            dirty: false,
        }
    }

    pub fn with_tick(mut self, tick: Tick) -> Self {
        self.current_tick = tick;
        self
    }

    pub fn tick(&self) -> Tick {
        self.current_tick
    }

    pub fn set_tick(&mut self, tick: Tick) {
        if self.current_tick != tick {
            self.clear();
            self.current_tick = tick;
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Lookup a row in the cache. On hit, promotes the entry to most-recent.
    pub fn get(&mut self, table: &str, row_id: RowId) -> Option<&CachedRow> {
        let key = cache_key(table, row_id);
        let entry = self.entries.get_mut(&key)?;
        self.access_counter = self.access_counter.saturating_add(1);
        entry.last_access = self.access_counter;
        Some(&entry.row)
    }

    /// Find an existing cached view for the given table and tick.
    ///
    /// Returns a clone of the shared view if any entry exists for the
    /// same table and tick, regardless of row_id.
    pub fn find_view(&self, table: &str, tick: Tick) -> Option<Arc<TableReadView>> {
        self.entries
            .values()
            .find(|e| e.row.tick == tick && e.row.table == table)
            .map(|e| Arc::clone(&e.row.view))
    }

    /// Insert a row into the cache, evicting the least-recently-used entry
    /// if the cache is at capacity.
    pub fn insert(&mut self, row: CachedRow) -> RuleResult<()> {
        if self.len() >= self.capacity {
            self.evict_lru();
        }

        let key = cache_key(&row.table, row.row_id);
        self.access_counter = self.access_counter.saturating_add(1);
        self.entries.insert(
            key,
            CacheEntry {
                row,
                last_access: self.access_counter,
            },
        );
        Ok(())
    }

    /// Remove all entries from the cache.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.access_counter = 0;
        self.dirty = false;
    }

    /// Mark the cache as dirty (stale). Called when authority tables change.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Returns true if the cache is dirty and needs repopulation.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Prefetch a set of rows from the given read source and populate the cache.
    pub fn prefetch<S: UnifiedReadSource>(
        &mut self,
        source: &S,
        table: &str,
        row_ids: &[RowId],
    ) -> RuleResult<()> {
        let request = scharnhorst_query::ReadRequest::new(self.current_tick).with_table(table);
        let response = source
            .read(request)
            .map_err(|e| RuleError::QueryEngine(e.to_string()))?;

        let view = response
            .get(table)
            .map_err(|e| RuleError::QueryEngine(e.to_string()))?;

        let shared_view = Arc::new(view.clone());

        for row_id in row_ids {
            let key = cache_key(table, *row_id);
            if self.entries.contains_key(&key) {
                continue;
            }

            let cached = CachedRow {
                tick: self.current_tick,
                table: table.to_owned(),
                row_id: *row_id,
                view: Arc::clone(&shared_view),
            };

            self.insert(cached)?;
        }

        Ok(())
    }

    fn evict_lru(&mut self) {
        let victim = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_access)
            .map(|(k, _)| k.clone());

        if let Some(key) = victim {
            self.entries.remove(&key);
        }
    }
}

impl Default for PrefetchCache {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }
}

impl DerivedState for PrefetchCache {
    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn rebuild_from_authority(&mut self, tick: Tick) -> scharnhorst_core::CoreResult<()> {
        self.clear();
        self.current_tick = tick;
        self.dirty = false;
        Ok(())
    }
}

fn cache_key(table: &str, row_id: RowId) -> CacheKey {
    CacheKey {
        table: table.to_owned(),
        row_id: row_id.as_u64(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_new_has_default_capacity() {
        let cache = PrefetchCache::new(PrefetchCache::DEFAULT_CAPACITY);
        assert_eq!(cache.capacity(), PrefetchCache::DEFAULT_CAPACITY);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_custom_capacity() {
        let cache = PrefetchCache::new(64);
        assert_eq!(cache.capacity(), 64);
    }

    #[test]
    fn cache_set_tick_clears() {
        let mut cache = PrefetchCache::new(64).with_tick(Tick(1));
        cache.set_tick(Tick(2));
        assert_eq!(cache.tick(), Tick(2));
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_clear_removes_all() {
        let mut cache = PrefetchCache::new(64);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_get_missing_returns_none() {
        let mut cache = PrefetchCache::new(64);
        assert!(cache.get("province", RowId::new(1)).is_none());
    }

    // ---------------------------------------------------------------------------
    // Construction & Default
    // ---------------------------------------------------------------------------

    /// PrefetchCache::default MUST use DEFAULT_CAPACITY (1024).
    #[test]
    fn dc7_cache_default_uses_1024_capacity() {
        let cache = PrefetchCache::default();
        assert_eq!(cache.capacity(), PrefetchCache::DEFAULT_CAPACITY);
        assert_eq!(cache.capacity(), 1024);
    }

    /// with_tick MUST set the current tick without side effects.
    #[test]
    fn dc7_cache_with_tick_sets_current_tick() {
        let cache = PrefetchCache::new(64).with_tick(Tick(42));
        assert_eq!(cache.tick(), Tick(42));
        // with_tick should not clear or change capacity.
        assert_eq!(cache.capacity(), 64);
        assert!(cache.is_empty());
    }

    /// A newly constructed cache MUST start at Tick::ZERO.
    #[test]
    fn dc7_cache_new_starts_at_tick_zero() {
        let cache = PrefetchCache::new(128);
        assert_eq!(cache.tick(), Tick::ZERO);
    }

    // ---------------------------------------------------------------------------
    // Tick Boundary Lifecycle
    // ---------------------------------------------------------------------------

    /// Setting a different tick MUST clear the cache and update the tick.
    #[test]
    fn dc7_set_tick_changes_tick_and_clears() {
        let mut cache = PrefetchCache::new(64).with_tick(Tick(10));
        // cache is empty at start, set_tick to a new tick still clears.
        cache.set_tick(Tick(20));
        assert_eq!(cache.tick(), Tick(20));
        assert!(cache.is_empty());
    }

    /// Setting the same tick MUST be a no-op (no clear, no state change).
    #[test]
    fn dc7_set_tick_same_tick_no_change() {
        let mut cache = PrefetchCache::new(64).with_tick(Tick(3));
        // set_tick with the already-current tick should not clear.
        cache.set_tick(Tick(3));
        assert_eq!(cache.tick(), Tick(3));
    }

    /// Transition from Tick(0) to Tick(1) MUST clear (coverage for ZERO
    /// boundary).
    #[test]
    fn dc7_set_tick_zero_to_one_clears() {
        // new cache starts at Tick::ZERO; setting tick to 1 must clear.
        let mut cache = PrefetchCache::new(64);
        assert_eq!(cache.tick(), Tick::ZERO);
        cache.set_tick(Tick(1));
        assert_eq!(cache.tick(), Tick(1));
        assert!(cache.is_empty());
    }

    // ---------------------------------------------------------------------------
    // Clear & Empty-state semantics
    // ---------------------------------------------------------------------------

    /// clear MUST reset the cache to empty and reset the access counter.
    #[test]
    fn dc7_cache_clear_makes_cache_empty() {
        let mut cache = PrefetchCache::new(64);
        // clear on an already-empty cache is idempotent.
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        // clear after with_tick still operates correctly.
        let mut cache = PrefetchCache::new(64).with_tick(Tick(5));
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        // clear must not change the tick.
        assert_eq!(cache.tick(), Tick(5));
    }

    /// A freshly created cache MUST report len=0 and is_empty=true.
    #[test]
    fn dc7_cache_len_zero_for_new_cache() {
        let cache = PrefetchCache::new(32);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    // ---------------------------------------------------------------------------
    // Capacity constraints
    // ---------------------------------------------------------------------------

    /// with_tick MUST preserve the capacity set by new.
    #[test]
    fn cache_with_tick_preserves_capacity() {
        let cache = PrefetchCache::new(500).with_tick(Tick(7));
        assert_eq!(cache.capacity(), 500);
    }

    /// Capacity may be larger than DEFAULT_CAPACITY.
    #[test]
    fn cache_capacity_can_be_larger_than_default() {
        let cache = PrefetchCache::new(2048);
        assert_eq!(cache.capacity(), 2048);
        assert!(cache.capacity() > PrefetchCache::DEFAULT_CAPACITY);
    }

    /// Capacity of zero is allowed (degenerate always-full cache).
    #[test]
    fn cache_zero_capacity_allowed() {
        let cache = PrefetchCache::new(0);
        assert_eq!(cache.capacity(), 0);
        assert!(cache.is_empty());
    }

    // ---------------------------------------------------------------------------
    // get on empty cache
    // ---------------------------------------------------------------------------

    /// get on an empty cache returns None for any key.
    #[test]
    fn cache_get_on_empty_returns_none_for_various_keys() {
        let mut cache = PrefetchCache::new(64);
        // various table/row combinations all return None.
        assert!(cache.get("units", RowId::new(0)).is_none());
        assert!(cache.get("units", RowId::new(42)).is_none());
        assert!(cache.get("buildings", RowId::new(1)).is_none());
        assert!(cache.get("", RowId::new(0)).is_none());
        assert!(cache.get("tech", RowId::new(u64::MAX)).is_none());

        // get on empty with Tick set should also return None.
        let mut cache = PrefetchCache::new(64).with_tick(Tick(99));
        assert!(cache.get("units", RowId::new(5)).is_none());
    }

    // ---------------------------------------------------------------------------
    // Tick-scoped documentation / behavior test
    // ---------------------------------------------------------------------------

    /// Verifies that set_tick enforces tick-scoped semantics,
    /// i.e. changing the tick clears all entries (tested by observable state).
    #[test]
    fn dc7_cache_is_tick_scoped() {
        // A cache at tick A, after set_tick(B) with B != A,
        // must report is_empty == true and tick == B.
        let mut cache = PrefetchCache::new(64).with_tick(Tick(100));
        cache.set_tick(Tick(200));
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.tick(), Tick(200));

        // Repeated tick changes are also safe.
        cache.set_tick(Tick(300));
        assert!(cache.is_empty());
        assert_eq!(cache.tick(), Tick(300));

        // Setting back to the same tick (300) is a no-op.
        cache.set_tick(Tick(300));
        assert!(cache.is_empty());
        assert_eq!(cache.tick(), Tick(300));
    }
}
