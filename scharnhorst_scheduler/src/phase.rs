use std::cmp::Ordering;

/// A simulation execution phase.
///
/// Phases are executed in strict order: `PreTick` -> `Economy` -> `Diplomacy` -> `PostTick`.
/// Systems within the same phase may run in parallel if their write sets are disjoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
 /// Setup and command ingestion at tick boundary.
    PreTick,
 /// Economy simulation (production, trade, resources).
    Economy,
 /// Diplomacy and political simulation.
    Diplomacy,
 /// Military and combat resolution.
    Military,
 /// Post-tick cleanup, commit, and snapshot refresh.
    PostTick,
}

impl Phase {
 /// Returns the canonical ordering index for this phase.
    pub fn order_index(self) -> u8 {
        match self {
            Phase::PreTick => 0,
            Phase::Economy => 1,
            Phase::Diplomacy => 2,
            Phase::Military => 3,
            Phase::PostTick => 4,
        }
    }

 /// Returns all phases in execution order.
    pub fn all_in_order() -> impl Iterator<Item = Phase> {
        [
            Phase::PreTick,
            Phase::Economy,
            Phase::Diplomacy,
            Phase::Military,
            Phase::PostTick,
        ]
        .into_iter()
    }

 /// Returns the phase that follows this one, if any.
    pub fn next(self) -> Option<Phase> {
        match self {
            Phase::PreTick => Some(Phase::Economy),
            Phase::Economy => Some(Phase::Diplomacy),
            Phase::Diplomacy => Some(Phase::Military),
            Phase::Military => Some(Phase::PostTick),
            Phase::PostTick => None,
        }
    }
}

impl PartialOrd for Phase {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Phase {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order_index().cmp(&other.order_index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_index_is_correct() {
        assert_eq!(Phase::PreTick.order_index(), 0);
        assert_eq!(Phase::Economy.order_index(), 1);
        assert_eq!(Phase::Diplomacy.order_index(), 2);
        assert_eq!(Phase::Military.order_index(), 3);
        assert_eq!(Phase::PostTick.order_index(), 4);
    }

    #[test]
    fn all_in_order_yields_correct_sequence() {
        let phases: Vec<Phase> = Phase::all_in_order().collect();
        assert_eq!(phases.len(), 5);
        assert_eq!(phases[0], Phase::PreTick);
        assert_eq!(phases[1], Phase::Economy);
        assert_eq!(phases[2], Phase::Diplomacy);
        assert_eq!(phases[3], Phase::Military);
        assert_eq!(phases[4], Phase::PostTick);
    }

    #[test]
    fn next_chain_is_correct() {
        let phases = [
            Phase::PreTick,
            Phase::Economy,
            Phase::Diplomacy,
            Phase::Military,
            Phase::PostTick,
        ];
        let chain: Vec<Option<Phase>> = phases.iter().map(|p| p.next()).collect();
        assert_eq!(
            chain,
            vec![
                Some(Phase::Economy),
                Some(Phase::Diplomacy),
                Some(Phase::Military),
                Some(Phase::PostTick),
                None,
            ]
        );
    }

    #[test]
    fn phase_sort_maintains_order() {
        let mut phases = vec![Phase::PostTick, Phase::PreTick, Phase::Military, Phase::Economy, Phase::Diplomacy];
        phases.sort();
        assert_eq!(phases, Phase::all_in_order().collect::<Vec<_>>());
    }

    #[test]
    fn phase_is_copy() {
        let a = Phase::Economy;
        let b = a;
        assert_eq!(a, b);
        assert_eq!(b.order_index(), 1);
    }
}
