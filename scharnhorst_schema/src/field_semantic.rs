use serde::{Deserialize, Serialize};

/// Describes the high-level meaning of a column so that systems
/// (query, rules, UI) can reason about it without hard-coding names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldSemantic {
 /// Primary key or row identifier.
    Id,
 /// Foreign key referencing another table.
    ForeignKey { target_table: String },
 /// Human-readable name or label.
    Name,
 /// A categorical tag / enum value.
    Tag,
 /// Position in 2D space (x, y).
    Position2D,
 /// Position in 3D space (x, y, z).
    Position3D,
 /// A fixed-point quantity (e.g. money, resources).
    Quantity,
 /// A percentage in the range [0, 100].
    Percent,
 /// A duration measured in simulation ticks.
    DurationTicks,
 /// A timestamp (simulation tick).
    Timestamp,
 /// Raw binary or text payload with no special meaning.
    Raw,
}

impl FieldSemantic {
 /// Returns true if the semantic implies a numeric representation.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Quantity | Self::Percent | Self::DurationTicks | Self::Timestamp | Self::Position2D | Self::Position3D
        )
    }

 /// Returns true if the semantic represents a spatial coordinate.
    pub fn is_spatial(&self) -> bool {
        matches!(self, Self::Position2D | Self::Position3D)
    }

 /// Returns true if the semantic is a reference to another entity.
    pub fn is_reference(&self) -> bool {
        matches!(self, Self::ForeignKey { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

 // ---- is_numeric ----

    #[test]
    fn id_is_not_numeric() {
        assert!(!FieldSemantic::Id.is_numeric());
    }

    #[test]
    fn foreign_key_is_not_numeric() {
        assert!(!FieldSemantic::ForeignKey {
            target_table: "t".into()
        }
        .is_numeric());
    }

    #[test]
    fn name_is_not_numeric() {
        assert!(!FieldSemantic::Name.is_numeric());
    }

    #[test]
    fn tag_is_not_numeric() {
        assert!(!FieldSemantic::Tag.is_numeric());
    }

    #[test]
    fn quantity_is_numeric() {
        assert!(FieldSemantic::Quantity.is_numeric());
    }

    #[test]
    fn percent_is_numeric() {
        assert!(FieldSemantic::Percent.is_numeric());
    }

    #[test]
    fn position2d_is_numeric() {
        assert!(FieldSemantic::Position2D.is_numeric());
    }

    #[test]
    fn position3d_is_numeric() {
        assert!(FieldSemantic::Position3D.is_numeric());
    }

    #[test]
    fn duration_ticks_is_numeric() {
        assert!(FieldSemantic::DurationTicks.is_numeric());
    }

    #[test]
    fn timestamp_is_numeric() {
        assert!(FieldSemantic::Timestamp.is_numeric());
    }

    #[test]
    fn raw_is_not_numeric() {
        assert!(!FieldSemantic::Raw.is_numeric());
    }

 // ---- is_spatial ----

    #[test]
    fn id_is_not_spatial() {
        assert!(!FieldSemantic::Id.is_spatial());
    }

    #[test]
    fn foreign_key_is_not_spatial() {
        assert!(!FieldSemantic::ForeignKey {
            target_table: "t".into()
        }
        .is_spatial());
    }

    #[test]
    fn name_is_not_spatial() {
        assert!(!FieldSemantic::Name.is_spatial());
    }

    #[test]
    fn tag_is_not_spatial() {
        assert!(!FieldSemantic::Tag.is_spatial());
    }

    #[test]
    fn quantity_is_not_spatial() {
        assert!(!FieldSemantic::Quantity.is_spatial());
    }

    #[test]
    fn percent_is_not_spatial() {
        assert!(!FieldSemantic::Percent.is_spatial());
    }

    #[test]
    fn position2d_is_spatial() {
        assert!(FieldSemantic::Position2D.is_spatial());
    }

    #[test]
    fn position3d_is_spatial() {
        assert!(FieldSemantic::Position3D.is_spatial());
    }

    #[test]
    fn duration_ticks_is_not_spatial() {
        assert!(!FieldSemantic::DurationTicks.is_spatial());
    }

    #[test]
    fn timestamp_is_not_spatial() {
        assert!(!FieldSemantic::Timestamp.is_spatial());
    }

    #[test]
    fn raw_is_not_spatial() {
        assert!(!FieldSemantic::Raw.is_spatial());
    }

 // ---- is_reference ----

    #[test]
    fn id_is_not_reference() {
        assert!(!FieldSemantic::Id.is_reference());
    }

    #[test]
    fn foreign_key_is_reference() {
        assert!(FieldSemantic::ForeignKey {
            target_table: "any_table".into()
        }
        .is_reference());
    }

    #[test]
    fn name_is_not_reference() {
        assert!(!FieldSemantic::Name.is_reference());
    }

    #[test]
    fn tag_is_not_reference() {
        assert!(!FieldSemantic::Tag.is_reference());
    }

    #[test]
    fn quantity_is_not_reference() {
        assert!(!FieldSemantic::Quantity.is_reference());
    }

    #[test]
    fn percent_is_not_reference() {
        assert!(!FieldSemantic::Percent.is_reference());
    }

    #[test]
    fn position2d_is_not_reference() {
        assert!(!FieldSemantic::Position2D.is_reference());
    }

    #[test]
    fn position3d_is_not_reference() {
        assert!(!FieldSemantic::Position3D.is_reference());
    }

    #[test]
    fn duration_ticks_is_not_reference() {
        assert!(!FieldSemantic::DurationTicks.is_reference());
    }

    #[test]
    fn timestamp_is_not_reference() {
        assert!(!FieldSemantic::Timestamp.is_reference());
    }

    #[test]
    fn raw_is_not_reference() {
        assert!(!FieldSemantic::Raw.is_reference());
    }
}
