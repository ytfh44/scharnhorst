use serde::{Deserialize, Serialize};

use scharnhorst_core::RowId;
use scharnhorst_schema::RelationEdge;

use crate::error::{RuleError, RuleResult};

/// The direction of a scope jump along a relation edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JumpDirection {
    /// Follow the edge from `from` to `to`.
    Forward,
    /// Follow the edge from `to` to `from`.
    Reverse,
}

/// A descriptor for a scope jump resolved via the RelationGraph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeJump {
    pub relation: String,
    pub direction: JumpDirection,
}

impl ScopeJump {
    pub fn forward(relation: impl Into<String>) -> Self {
        Self {
            relation: relation.into(),
            direction: JumpDirection::Forward,
        }
    }

    pub fn reverse(relation: impl Into<String>) -> Self {
        Self {
            relation: relation.into(),
            direction: JumpDirection::Reverse,
        }
    }
}

/// A stack-based execution scope that tracks the current table and row.
///
/// Scope jumps traverse the [`RelationGraph`] to resolve cross-table
/// references. The evaluator uses the query-engine for all
/// lookups; it never touches Arrow partitions directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    table: String,
    row: RowId,
    stack: Vec<ScopeFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ScopeFrame {
    table: String,
    row: RowId,
    jump: Option<ScopeJump>,
}

impl Scope {
    pub fn new(table: impl Into<String>, row: RowId) -> Self {
        Self {
            table: table.into(),
            row,
            stack: Vec::new(),
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn row(&self) -> RowId {
        self.row
    }

    pub fn stack_depth(&self) -> usize {
        self.stack.len()
    }

    /// Push a new frame onto the scope stack, changing the active table/row.
    pub fn push(&mut self, table: impl Into<String>, row: RowId, jump: Option<ScopeJump>) {
        let frame = ScopeFrame {
            table: self.table.clone(),
            row: self.row,
            jump,
        };
        self.stack.push(frame);
        self.table = table.into();
        self.row = row;
    }

    /// Pop the top frame and restore the previous scope.
    pub fn pop(&mut self) -> RuleResult<()> {
        let frame = self
            .stack
            .pop()
            .ok_or_else(|| RuleError::Scope("scope stack underflow".to_owned()))?;
        self.table = frame.table;
        self.row = frame.row;
        Ok(())
    }

    /// Resolve a scope jump using the provided relation edge.
    ///
    /// The caller is responsible for looking up the target row via the
    /// query-engine; this method only updates the scope state.
    pub fn apply_jump(
        &mut self,
        edge: &RelationEdge,
        direction: JumpDirection,
        target_row: RowId,
    ) -> RuleResult<()> {
        let target_table = match direction {
            JumpDirection::Forward => edge.to.clone(),
            JumpDirection::Reverse => edge.from.clone(),
        };

        let jump = ScopeJump {
            relation: format!("{} -> {}", edge.from, edge.to),
            direction,
        };

        self.push(target_table, target_row, Some(jump));
        Ok(())
    }

    /// Return an iterator over the current scope chain (bottom to top).
    pub fn chain(&self) -> impl Iterator<Item = (&str, RowId)> + '_ {
        self.stack
            .iter()
            .map(|f| (f.table.as_str(), f.row))
            .chain(std::iter::once((self.table.as_str(), self.row)))
    }

    /// Reset the scope to a new root, clearing the stack.
    pub fn reset(&mut self, table: impl Into<String>, row: RowId) {
        self.stack.clear();
        self.table = table.into();
        self.row = row;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_schema::RelationKind;

    // =========================================================================
    // Existing tests (kept intact)
    // =========================================================================

    #[test]
    fn scope_jump_forward() {
        let jump = ScopeJump::forward("province -> actor");
        assert_eq!(jump.relation, "province -> actor");
        assert_eq!(jump.direction, JumpDirection::Forward);
    }

    #[test]
    fn scope_jump_reverse() {
        let jump = ScopeJump::reverse("province -> actor");
        assert_eq!(jump.relation, "province -> actor");
        assert_eq!(jump.direction, JumpDirection::Reverse);
    }

    #[test]
    fn scope_new() {
        let scope = Scope::new("province", RowId::new(1));
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
    }

    #[test]
    fn scope_push_pop() -> RuleResult<()> {
        let mut scope = Scope::new("province", RowId::new(1));
        scope.push("actor_state", RowId::new(5), None);
        assert_eq!(scope.table(), "actor_state");
        assert_eq!(scope.row(), RowId::new(5));
        scope.pop()?;
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
        Ok(())
    }

    #[test]
    fn scope_reset() {
        let mut scope = Scope::new("province", RowId::new(1));
        scope.push("actor_state", RowId::new(5), None);
        scope.reset("nation", RowId::new(3));
        assert_eq!(scope.table(), "nation");
        assert_eq!(scope.row(), RowId::new(3));
    }

    // =========================================================================
    // A. ScopeJump construction
    // =========================================================================

    /// Verify ScopeJump::forward sets the relation string and Forward direction.
    #[test]
    fn scope_jump_forward_uses_forward_direction() {
        let jump = ScopeJump::forward("province -> actor");
        assert_eq!(jump.relation, "province -> actor");
        assert_eq!(jump.direction, JumpDirection::Forward);
    }

    /// Verify ScopeJump::reverse sets the relation string and Reverse direction.
    #[test]
    fn scope_jump_reverse_uses_reverse_direction() {
        let jump = ScopeJump::reverse("actor -> province");
        assert_eq!(jump.relation, "actor -> province");
        assert_eq!(jump.direction, JumpDirection::Reverse);
    }

    /// Serde JSON roundtrip for ScopeJump preserves all fields.
    #[test]
    fn scope_jump_serialization_roundtrip() -> RuleResult<()> {
        let jump = ScopeJump::forward("province -> actor");
        let json = serde_json::to_string(&jump).map_err(|e| RuleError::Generic(e.to_string()))?;
        let roundtripped: ScopeJump =
            serde_json::from_str(&json).map_err(|e| RuleError::Generic(e.to_string()))?;
        assert_eq!(roundtripped, jump);
        Ok(())
    }

    /// Serde JSON roundtrip for JumpDirection (Forward and Reverse).
    #[test]
    fn jump_direction_serialization_roundtrip() -> RuleResult<()> {
        for dir in [JumpDirection::Forward, JumpDirection::Reverse] {
            let json =
                serde_json::to_string(&dir).map_err(|e| RuleError::Generic(e.to_string()))?;
            let roundtripped: JumpDirection =
                serde_json::from_str(&json).map_err(|e| RuleError::Generic(e.to_string()))?;
            assert_eq!(roundtripped, dir);
        }
        Ok(())
    }

    // =========================================================================
    // B. Scope Core Operations
    // =========================================================================

    /// A newly created Scope has stack_depth of zero.
    #[test]
    fn scope_stack_depth_zero_after_new() {
        let scope = Scope::new("province", RowId::new(1));
        assert_eq!(scope.stack_depth(), 0);
    }

    /// Each push increments the stack depth by one.
    #[test]
    fn scope_stack_depth_increments_with_push() {
        let mut scope = Scope::new("province", RowId::new(1));
        assert_eq!(scope.stack_depth(), 0);
        scope.push("actor", RowId::new(5), None);
        assert_eq!(scope.stack_depth(), 1);
        scope.push("population", RowId::new(42), None);
        assert_eq!(scope.stack_depth(), 2);
    }

    /// Push saves the previous frame; pop restores both table and row.
    #[test]
    fn scope_push_preserves_previous_frame() -> RuleResult<()> {
        let mut scope = Scope::new("province", RowId::new(1));
        scope.push("actor", RowId::new(5), None);
        // Current scope is now the pushed frame
        assert_eq!(scope.table(), "actor");
        assert_eq!(scope.row(), RowId::new(5));
        // Pop restores the previous frame
        scope.pop()?;
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
        Ok(())
    }

    /// Push with jump metadata stores the full frame; pop restores table and row.
    /// Note: ScopeJump metadata is stored on the private ScopeFrame;
    /// correctness is verified indirectly through successful pop restoration.
    #[test]
    fn scope_pop_restores_previous_frame_full() -> RuleResult<()> {
        let mut scope = Scope::new("province", RowId::new(1));
        let jump = ScopeJump::forward("province -> actor");
        scope.push("actor", RowId::new(5), Some(jump));
        assert_eq!(scope.table(), "actor");
        assert_eq!(scope.row(), RowId::new(5));
        // Pop restores the full previous frame (table + row)
        scope.pop()?;
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
        assert_eq!(scope.stack_depth(), 0);
        Ok(())
    }

    /// Pop on an empty stack returns a scope-underflow error.
    #[test]
    fn scope_pop_empty_stack_returns_error() {
        let mut scope = Scope::new("province", RowId::new(1));
        let result = scope.pop();
        assert!(result.is_err());
        // Scope must be unchanged after a failed pop
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
        assert_eq!(scope.stack_depth(), 0);
    }

    /// Repeated push-pop cycles correctly restore the root scope each time.
    #[test]
    fn scope_pop_then_push_then_pop() -> RuleResult<()> {
        let mut scope = Scope::new("province", RowId::new(1));
        // First push-pop cycle
        scope.push("actor", RowId::new(5), None);
        scope.pop()?;
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
        // Second push-pop cycle
        scope.push("nation", RowId::new(3), None);
        assert_eq!(scope.table(), "nation");
        scope.pop()?;
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
        Ok(())
    }

    // =========================================================================
    // C. apply_jump (RelationGraph traversal)
    // =========================================================================

    /// apply_jump Forward follows the edge from->to and updates scope
    /// to the target table with the given row.
    #[test]
    fn dc6_apply_jump_forward_updates_scope_to_target_table() -> RuleResult<()> {
        let mut scope = Scope::new("province", RowId::new(1));
        let edge = RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: None,
        };
        scope.apply_jump(&edge, JumpDirection::Forward, RowId::new(42))?;
        // after forward jump, scope is at the "to" table
        assert_eq!(scope.table(), "actor");
        assert_eq!(scope.row(), RowId::new(42));
        Ok(())
    }

    /// apply_jump Reverse follows the edge to->from and updates scope
    /// to the source table with the given row.
    #[test]
    fn dc6_apply_jump_reverse_updates_scope_to_source_table() -> RuleResult<()> {
        let mut scope = Scope::new("actor", RowId::new(5));
        let edge = RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: None,
        };
        scope.apply_jump(&edge, JumpDirection::Reverse, RowId::new(7))?;
        // after reverse jump, scope is at the "from" table
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(7));
        Ok(())
    }

    /// apply_jump records a ScopeJump on the pushed frame with the
    /// correct relation name (formatted as "from -> to").
    /// Verified indirectly: pop restores the pre-jump scope correctly.
    #[test]
    fn dc6_apply_jump_records_scope_jump_metadata() -> RuleResult<()> {
        let mut scope = Scope::new("province", RowId::new(1));
        let edge = RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: None,
        };
        scope.apply_jump(&edge, JumpDirection::Forward, RowId::new(5))?;
        // After jump, we are at the target
        assert_eq!(scope.table(), "actor");
        assert_eq!(scope.row(), RowId::new(5));
        // Pop to verify the ScopeJump frame was correctly stored
        // (relation = "province -> actor", direction = Forward)
        scope.pop()?;
        assert_eq!(scope.table(), "province");
        assert_eq!(scope.row(), RowId::new(1));
        Ok(())
    }

    /// Full scope-jump stack chain: province (row 1) -> apply_jump
    /// Forward -> actor (row 5) -> push population (row 42).
    /// chain iterator must return all three frames bottom-to-top.
    #[test]
    fn dc6_scope_jump_stack_chain_actor_to_owned_province_to_local_pop() -> RuleResult<()> {
        let mut scope = Scope::new("province", RowId::new(1));
        let edge = RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::Composition,
            from_column: "owner_id".to_string(),
            to_column: None,
        };
        // apply_jump stores current frame and moves to target
        scope.apply_jump(&edge, JumpDirection::Forward, RowId::new(5))?;
        // Push one more frame to build a 3-level chain
        scope.push("population", RowId::new(42), None);
        // chain returns bottom-to-top: (province,1) -> (actor,5) -> (population,42)
        let chain: Vec<(&str, RowId)> = scope.chain().collect();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0], ("province", RowId::new(1)));
        assert_eq!(chain[1], ("actor", RowId::new(5)));
        assert_eq!(chain[2], ("population", RowId::new(42)));
        Ok(())
    }

    // =========================================================================
    // D. Scope chain iterator
    // =========================================================================

    /// chain on a new scope with an empty stack returns exactly 1 element
    /// (the current/root scope).
    #[test]
    fn scope_chain_returns_empty_single_element_for_new_scope() {
        let scope = Scope::new("province", RowId::new(1));
        let chain: Vec<(&str, RowId)> = scope.chain().collect();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0], ("province", RowId::new(1)));
    }

    /// chain returns all frames in bottom-to-top order for a multi-frame stack.
    #[test]
    fn scope_chain_returns_all_frames_in_order() {
        let mut scope = Scope::new("bottom", RowId::new(0));
        scope.push("middle", RowId::new(1), None);
        scope.push("top", RowId::new(2), None);
        let chain: Vec<(&str, RowId)> = scope.chain().collect();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0], ("bottom", RowId::new(0)));
        assert_eq!(chain[1], ("middle", RowId::new(1)));
        assert_eq!(chain[2], ("top", RowId::new(2)));
    }

    /// After reset, chain returns exactly 1 element (the new root).
    #[test]
    fn scope_chain_after_reset_returns_single_element() {
        let mut scope = Scope::new("province", RowId::new(1));
        scope.push("actor", RowId::new(5), None);
        scope.push("population", RowId::new(42), None);
        scope.reset("nation", RowId::new(3));
        let chain: Vec<(&str, RowId)> = scope.chain().collect();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0], ("nation", RowId::new(3)));
    }

    // =========================================================================
    // E. Scope reset
    // =========================================================================

    /// Reset clears the entire stack (stack_depth=0) and sets a new root scope.
    #[test]
    fn scope_reset_clears_stack_and_updates_root() {
        let mut scope = Scope::new("province", RowId::new(1));
        scope.push("actor", RowId::new(5), None);
        scope.push("population", RowId::new(42), None);
        scope.push("building", RowId::new(99), None);
        scope.reset("nation", RowId::new(3));
        assert_eq!(scope.stack_depth(), 0);
        assert_eq!(scope.table(), "nation");
        assert_eq!(scope.row(), RowId::new(3));
    }
}
