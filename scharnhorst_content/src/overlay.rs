use std::collections::HashMap;

use crate::error::{ContentError, ContentResult};

/// Strategy for merging a mod overlay value into base content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeStrategy {
    /// Replace the base value entirely.
    Replace,
    /// Merge collections by appending mod entries.
    Append,
    /// Merge collections by prepending mod entries.
    Prepend,
    /// Numerically add mod value to base value.
    Add,
    /// Take the minimum of base and mod values.
    Min,
    /// Take the maximum of base and mod values.
    Max,
}

/// A single overlay entry defining how a mod modifies a base value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayEntry {
    /// Dot-separated path within the content object (e.g. "actor.FRA.stability").
    pub path: String,
    /// The raw value to apply (interpretation depends on `strategy`).
    pub value: String,
    /// How this entry should be merged with the base value.
    pub strategy: MergeStrategy,
}

/// Represents a single mod's overlay layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayLayer {
    /// Mod identifier (e.g. "EuropaBarbarorum").
    pub mod_id: String,
    /// Mod priority; lower values apply first.
    pub priority: i32,
    /// Entries keyed by path for fast lookup.
    pub entries: HashMap<String, OverlayEntry>,
}

impl OverlayLayer {
    pub fn new(mod_id: impl Into<String>, priority: i32) -> Self {
        Self {
            mod_id: mod_id.into(),
            priority,
            entries: HashMap::new(),
        }
    }

    pub fn with_entry(mut self, entry: OverlayEntry) -> Self {
        self.entries.insert(entry.path.clone(), entry);
        self
    }

    pub fn get(&self, path: &str) -> Option<&OverlayEntry> {
        self.entries.get(path)
    }
}

/// Resolves the final value for a content path by applying overlays
/// in priority order over a base value.
#[derive(Debug, Clone, Default)]
pub struct OverlayResolver {
    base: HashMap<String, String>,
    layers: Vec<OverlayLayer>,
}

impl OverlayResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_base(mut self, path: impl Into<String>, value: impl Into<String>) -> Self {
        self.base.insert(path.into(), value.into());
        self
    }

    pub fn add_layer(&mut self, layer: OverlayLayer) {
        self.layers.push(layer);
        // Higher priority values should be applied later, so sort ascending.
        self.layers.sort_by_key(|l| l.priority);
    }

    /// Returns the fully-resolved value for `path`, or an error if none exists.
    pub fn resolve(&self, path: &str) -> ContentResult<String> {
        let mut current = self
            .base
            .get(path)
            .cloned()
            .ok_or_else(|| ContentError::OverlayNotFound(path.to_owned()))?;

        for layer in &self.layers {
            if let Some(entry) = layer.get(path) {
                current = apply_strategy(&current, &entry.value, entry.strategy)?;
            }
        }

        Ok(current)
    }

    /// Returns all paths known to the resolver (base + overlays).
    pub fn known_paths(&self) -> impl Iterator<Item = &str> {
        let base_keys = self.base.keys().map(|s| s.as_str());
        let overlay_keys = self
            .layers
            .iter()
            .flat_map(|l| l.entries.keys().map(|s| s.as_str()));
        base_keys.chain(overlay_keys)
    }

    /// Apply all overlays at once, returning resolved values for every known path.
    pub fn apply(&self) -> ContentResult<HashMap<String, String>> {
        let paths: Vec<String> = self.known_paths().map(|s| s.to_owned()).collect();
        let mut result = HashMap::with_capacity(paths.len());
        for path in &paths {
            let value = self.resolve(path)?;
            result.insert(path.clone(), value);
        }
        Ok(result)
    }
}

fn apply_strategy(base: &str, overlay: &str, strategy: MergeStrategy) -> ContentResult<String> {
    match strategy {
        MergeStrategy::Replace => Ok(overlay.to_owned()),
        MergeStrategy::Append => Ok(format!("{}{}", base, overlay)),
        MergeStrategy::Prepend => Ok(format!("{}{}", overlay, base)),
        MergeStrategy::Add => {
            let b = base.parse::<i64>().map_err(|_| {
                ContentError::OverlayConflict(format!("cannot add non-integer base: {}", base))
            })?;
            let o = overlay.parse::<i64>().map_err(|_| {
                ContentError::OverlayConflict(format!(
                    "cannot add non-integer overlay: {}",
                    overlay
                ))
            })?;
            Ok((b + o).to_string())
        }
        MergeStrategy::Min => {
            let b = base.parse::<i64>().map_err(|_| {
                ContentError::OverlayConflict(format!("cannot min non-integer base: {}", base))
            })?;
            let o = overlay.parse::<i64>().map_err(|_| {
                ContentError::OverlayConflict(format!(
                    "cannot min non-integer overlay: {}",
                    overlay
                ))
            })?;
            Ok(b.min(o).to_string())
        }
        MergeStrategy::Max => {
            let b = base.parse::<i64>().map_err(|_| {
                ContentError::OverlayConflict(format!("cannot max non-integer base: {}", base))
            })?;
            let o = overlay.parse::<i64>().map_err(|_| {
                ContentError::OverlayConflict(format!(
                    "cannot max non-integer overlay: {}",
                    overlay
                ))
            })?;
            Ok(b.max(o).to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_returns_all_resolved_paths() {
        let resolver = OverlayResolver::new()
            .with_base("a", "1")
            .with_base("b", "2");
        let result = resolver.apply().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("a").unwrap(), "1");
        assert_eq!(result.get("b").unwrap(), "2");
    }

    #[test]
    fn apply_with_replace_overlay() {
        let mut resolver = OverlayResolver::new().with_base("x", "old");
        let layer = OverlayLayer::new("mod_a", 1).with_entry(OverlayEntry {
            path: "x".to_owned(),
            value: "new".to_owned(),
            strategy: MergeStrategy::Replace,
        });
        resolver.add_layer(layer);
        let result = resolver.apply().unwrap();
        assert_eq!(result.get("x").unwrap(), "new");
    }

    #[test]
    fn apply_empty_resolver() {
        let resolver = OverlayResolver::new();
        let result = resolver.apply().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn apply_strategy_prepend() {
        assert_eq!(
            apply_strategy("base", "pre", MergeStrategy::Prepend).unwrap(),
            "prebase"
        );
    }

    #[test]
    fn apply_strategy_min_max() {
        assert_eq!(apply_strategy("10", "5", MergeStrategy::Min).unwrap(), "5");
        assert_eq!(
            apply_strategy("10", "20", MergeStrategy::Max).unwrap(),
            "20"
        );
    }

    #[test]
    fn apply_strategy_add_negative() {
        assert_eq!(
            apply_strategy("50", "-10", MergeStrategy::Add).unwrap(),
            "40"
        );
    }

    #[test]
    fn apply_strategy_invalid_integer() {
        let err = apply_strategy("abc", "1", MergeStrategy::Add).unwrap_err();
        assert!(matches!(err, ContentError::OverlayConflict(_)));
    }

    #[test]
    fn layer_get_miss() {
        let layer = OverlayLayer::new("test", 0);
        assert!(layer.get("missing").is_none());
    }

    #[test]
    fn multiple_layers_apply_in_order() {
        let mut resolver = OverlayResolver::new().with_base("v", "0");
        resolver.add_layer(OverlayLayer::new("layer1", 0).with_entry(OverlayEntry {
            path: "v".to_owned(),
            value: "10".to_owned(),
            strategy: MergeStrategy::Add,
        }));
        resolver.add_layer(OverlayLayer::new("layer2", 1).with_entry(OverlayEntry {
            path: "v".to_owned(),
            value: "5".to_owned(),
            strategy: MergeStrategy::Add,
        }));
        let result = resolver.apply().unwrap();
        assert_eq!(result.get("v").unwrap(), "15");
    }
}
