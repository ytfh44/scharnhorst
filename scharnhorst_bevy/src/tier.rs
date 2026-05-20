use bevy::prelude::Resource;
use scharnhorst_core::{StateTier, TierRegistry};

/// Standard bevy-bridge ephemeral table names.
///
/// These tables hold per-tick transient state that is neither persisted
/// nor rebuilt across ticks. They are discarded at tick end and regenerated
/// each frame from input events.
pub const EPH_HOVER: &str = "bevy_hover";
pub const EPH_SELECTION: &str = "bevy_selection";
pub const EPH_CAMERA: &str = "bevy_camera";

/// Creates a [`TierRegistry`] pre-populated with the standard
/// bevy-bridge ephemeral table registrations.
///
/// # Usage
///
/// ```rust,ignore
/// // During app initialization:
/// let registry = register_bevy_ephemeral_tables();
/// app.insert_resource(registry);
/// ```
pub fn register_bevy_ephemeral_tables() -> TierRegistry {
    let mut registry = TierRegistry::new();
    let _ = registry.register(
        EPH_HOVER,
        StateTier::Ephemeral,
        "bevy_bridge",
        "hovered entity tracking (transient per tick)",
    );
    let _ = registry.register(
        EPH_SELECTION,
        StateTier::Ephemeral,
        "bevy_bridge",
        "selected entity set (transient per tick)",
    );
    let _ = registry.register(
        EPH_CAMERA,
        StateTier::Ephemeral,
        "bevy_bridge",
        "active camera state (position, rotation, zoom)",
    );
    registry
}

/// A Bevy [`Resource`] wrapper around [`TierRegistry`] for the bridge.
///
/// The scheduler and save-system consult this registry to determine
/// per-table persistence and rebuild behavior.
#[derive(Debug, Clone, Resource)]
pub struct BridgeTierRegistry(pub TierRegistry);

impl Default for BridgeTierRegistry {
    fn default() -> Self {
        Self(register_bevy_ephemeral_tables())
    }
}

impl BridgeTierRegistry {
    /// Create a new bridge tier registry with the standard ephemeral registrations.
    pub fn new() -> Self {
        Self::default()
    }

    /// Access the inner `TierRegistry`.
    pub fn inner(&self) -> &TierRegistry {
        &self.0
    }

    /// Mutably access the inner `TierRegistry`.
    pub fn inner_mut(&mut self) -> &mut TierRegistry {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_three_ephemeral_tables() {
        let registry = register_bevy_ephemeral_tables();
        assert_eq!(registry.table_count(), 3);

        let hover = registry.get(EPH_HOVER).unwrap();
        assert_eq!(hover.tier, StateTier::Ephemeral);
        assert_eq!(hover.owner_subsystem, "bevy_bridge");

        let sel = registry.get(EPH_SELECTION).unwrap();
        assert_eq!(sel.tier, StateTier::Ephemeral);

        let cam = registry.get(EPH_CAMERA).unwrap();
        assert_eq!(cam.tier, StateTier::Ephemeral);
    }

    #[test]
    fn bridge_tier_registry_default() {
        let btr = BridgeTierRegistry::default();
        assert_eq!(btr.inner().table_count(), 3);
        assert!(!btr.inner().is_authority(EPH_HOVER));
        assert!(!btr.inner().is_authority(EPH_SELECTION));
        assert!(!btr.inner().is_authority(EPH_CAMERA));
    }

    #[test]
    fn bridge_tier_registry_new_equals_default() {
        let a = BridgeTierRegistry::new();
        let b = BridgeTierRegistry::default();
        assert_eq!(a.inner().table_count(), b.inner().table_count());
    }
}
