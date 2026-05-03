#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BridgeOutputOwner {
    /// Current cold-start/default path: bridge owns terminal delivery and
    /// finalizes the turn itself.
    Bridge,
    /// Current warm-follow-up handoff: bridge received `TmuxReady` without
    /// response text and leaves placeholder/terminal delivery to the watcher.
    LegacyTmuxHandoff,
    /// Watcher already owns live assistant relay for this turn; bridge must not
    /// duplicate the terminal response.
    WatcherRelay,
}

pub(super) fn classify_bridge_output_owner(
    rx_disconnected: bool,
    tmux_handed_off: bool,
    bridge_response_empty: bool,
    bridge_relay_delegated_to_watcher: bool,
) -> BridgeOutputOwner {
    if bridge_relay_delegated_to_watcher {
        BridgeOutputOwner::WatcherRelay
    } else if rx_disconnected && tmux_handed_off && bridge_response_empty {
        BridgeOutputOwner::LegacyTmuxHandoff
    } else {
        BridgeOutputOwner::Bridge
    }
}

impl BridgeOutputOwner {
    pub(super) fn skips_bridge_spinner_cleanup(self) -> bool {
        matches!(
            self,
            BridgeOutputOwner::LegacyTmuxHandoff | BridgeOutputOwner::WatcherRelay
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_output_owner_characterizes_current_cold_start_delivery() {
        assert_eq!(
            classify_bridge_output_owner(false, false, false, false),
            BridgeOutputOwner::Bridge,
            "cold/default turns keep bridge-owned terminal delivery today"
        );
        assert_eq!(
            classify_bridge_output_owner(true, false, true, false),
            BridgeOutputOwner::Bridge,
            "rx disconnect alone is not a watcher handoff without TmuxReady"
        );
    }

    #[test]
    fn bridge_output_owner_characterizes_warm_tmux_handoff_contract() {
        let owner = classify_bridge_output_owner(true, true, true, false);
        assert_eq!(owner, BridgeOutputOwner::LegacyTmuxHandoff);
        assert!(
            owner.skips_bridge_spinner_cleanup(),
            "warm handoff leaves the visible turn lifecycle to the watcher"
        );
    }

    #[test]
    fn bridge_output_owner_prefers_explicit_watcher_relay_delegation() {
        let owner = classify_bridge_output_owner(true, true, true, true);
        assert_eq!(owner, BridgeOutputOwner::WatcherRelay);
        assert!(
            owner.skips_bridge_spinner_cleanup(),
            "watcher relay delegation must not duplicate bridge terminal delivery"
        );
    }
}
