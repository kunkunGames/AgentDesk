//! Provider-keyed Discord channel map. Unknown keys round-trip; writes store
//! canonical ids when an alias is recognized.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::AgentChannel;

/// Canonical alias recognized without depending on the provider crate.
const ALIASES: &[(&str, &str)] = &[("agy", "antigravity")];

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct AgentChannels {
    inner: BTreeMap<String, AgentChannel>,
}

impl AgentChannels {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn canonicalize_key(raw: &str) -> String {
        let key = raw.trim().to_ascii_lowercase();
        ALIASES
            .iter()
            .find(|(alias, _)| *alias == key)
            .map(|(_, canonical)| (*canonical).to_string())
            .unwrap_or(key)
    }

    pub fn get(&self, id: &str) -> Option<&AgentChannel> {
        let key = Self::canonicalize_key(id);
        self.inner.get(&key)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut AgentChannel> {
        let key = Self::canonicalize_key(id);
        self.inner.get_mut(&key)
    }

    pub fn insert(&mut self, id: impl AsRef<str>, channel: AgentChannel) -> Option<AgentChannel> {
        self.inner
            .insert(Self::canonicalize_key(id.as_ref()), channel)
    }

    pub fn remove(&mut self, id: &str) -> Option<AgentChannel> {
        self.inner.remove(&Self::canonicalize_key(id))
    }

    pub fn contains_key(&self, id: &str) -> bool {
        self.inner.contains_key(&Self::canonicalize_key(id))
    }

    pub fn with(mut self, id: impl AsRef<str>, channel: AgentChannel) -> Self {
        self.insert(id, channel);
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &AgentChannel)> {
        self.inner
            .iter()
            .map(|(key, channel)| (key.as_str(), channel))
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(String::as_str)
    }

    pub fn is_map_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// True when no channel has a usable target. Empty maps and maps of empty
    /// channels both count as empty for serde skip.
    pub fn is_empty(&self) -> bool {
        self.inner
            .values()
            .all(|channel| channel.target().is_none())
    }

    pub fn first_present(&self) -> Option<(&str, &AgentChannel)> {
        self.iter().next()
    }

    pub fn upsert<F>(&mut self, id: &str, update: F)
    where
        F: FnOnce(Option<AgentChannel>) -> Option<AgentChannel>,
    {
        if let Some(channel) = update(self.remove(id)) {
            self.insert(id, channel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_five_keys_round_trip() {
        let yaml = "claude: ch-cc\ncodex: ch-cdx\ngemini: ch-gm\nopencode: ch-oc\nqwen: ch-qw\n";
        let channels: AgentChannels = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            channels
                .get("claude")
                .and_then(AgentChannel::target)
                .as_deref(),
            Some("ch-cc")
        );
        assert_eq!(
            channels
                .get("qwen")
                .and_then(AgentChannel::target)
                .as_deref(),
            Some("ch-qw")
        );
        let encoded = serde_yaml::to_string(&channels).unwrap();
        assert!(encoded.contains("claude: ch-cc"));
        assert!(encoded.contains("opencode: ch-oc"));
    }

    #[test]
    fn grok_and_antigravity_round_trip_without_new_fields() {
        let yaml = "grok: ch-gx\nantigravity: ch-ag\n";
        let channels: AgentChannels = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            channels
                .get("grok")
                .and_then(AgentChannel::target)
                .as_deref(),
            Some("ch-gx")
        );
        assert_eq!(
            channels
                .get("antigravity")
                .and_then(AgentChannel::target)
                .as_deref(),
            Some("ch-ag")
        );
        assert_eq!(
            channels
                .get("agy")
                .and_then(AgentChannel::target)
                .as_deref(),
            Some("ch-ag")
        );
    }

    #[test]
    fn write_canonicalizes_agy_alias() {
        let mut channels = AgentChannels::new();
        channels.insert("agy", AgentChannel::from("ch-ag"));
        assert!(channels.contains_key("antigravity"));
        assert!(!channels.inner.contains_key("agy"));
    }

    #[test]
    fn unknown_keys_are_preserved() {
        let yaml = "claude: ch-cc\nfuture-cli: ch-x\n";
        let channels: AgentChannels = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            channels
                .get("future-cli")
                .and_then(AgentChannel::target)
                .as_deref(),
            Some("ch-x")
        );
        let encoded = serde_yaml::to_string(&channels).unwrap();
        assert!(encoded.contains("future-cli: ch-x"));
    }
}
