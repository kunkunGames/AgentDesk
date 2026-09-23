//! Boot-time module selection, independent from cluster lease ownership.
use serde::{Deserialize, Serialize};

use super::{ClusterConfig, ClusterIntakeRoutingMode, ClusterRole};

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfile {
    #[default]
    Full,
    Runner,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct RuntimeModulePlan {
    pub gateway: bool,
    pub voice: bool,
    pub dashboard: bool,
    pub admin_api: bool,
    pub hub_services: bool,
}

impl RuntimeProfile {
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full)
    }

    pub fn modules(self) -> RuntimeModulePlan {
        let full = self.is_full();
        RuntimeModulePlan {
            gateway: full,
            voice: full,
            dashboard: full,
            admin_api: full,
            hub_services: full,
        }
    }

    pub(super) fn validate(self, cluster: &ClusterConfig) -> anyhow::Result<()> {
        if self == Self::Runner {
            anyhow::ensure!(
                cluster.enabled && cluster.role == ClusterRole::Runner,
                "cluster.runtime_profile=runner requires cluster.enabled=true and role=runner"
            );
            anyhow::ensure!(
                cluster.intake_routing.enabled
                    && cluster.intake_routing.mode != ClusterIntakeRoutingMode::Disabled,
                "cluster.runtime_profile=runner requires enabled intake routing in observe or enforce mode"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_cluster_roles_keep_single_machine_full_runtime() {
        let cluster: ClusterConfig = serde_yaml::from_str("{}").unwrap();
        assert!(!cluster.enabled);
        assert_eq!(cluster.runtime_profile, RuntimeProfile::Full);
        assert!(cluster.runtime_profile.modules().gateway);
        assert!(cluster.runtime_profile.modules().admin_api);
        assert!(cluster.runtime_profile.modules().dashboard);
        assert!(!cluster.intake_routing.capacity_aware);
        assert_eq!(cluster.execution_slots, None);
        assert!(cluster.runtime_profile.validate(&cluster).is_ok());
    }

    #[test]
    fn runner_profile_is_explicit_validated_and_keeps_full_default() {
        for role in ["hub", "runner", "auto"] {
            let config: ClusterConfig = serde_yaml::from_str(&format!("role: {role}")).unwrap();
            assert_eq!(config.runtime_profile, RuntimeProfile::Full);
            assert!(config.runtime_profile.modules().gateway);
            assert!(config.runtime_profile.validate(&config).is_ok());
        }
        assert!(serde_yaml::from_str::<ClusterConfig>("runtime_profile: typo").is_err());
        assert!(serde_yaml::from_str::<ClusterConfig>("runtime_profile: worker").is_err());
        let mut cluster = ClusterConfig {
            runtime_profile: RuntimeProfile::Runner,
            enabled: true,
            role: ClusterRole::Runner,
            ..Default::default()
        };
        assert!(cluster.runtime_profile.validate(&cluster).is_err());
        cluster.intake_routing.enabled = true;
        assert!(cluster.runtime_profile.validate(&cluster).is_ok());
        assert_eq!(
            serde_json::to_value(cluster.runtime_profile.modules()).unwrap(),
            serde_json::json!({"gateway":false,"voice":false,"dashboard":false,"admin_api":false,"hub_services":false})
        );
        cluster.role = ClusterRole::Auto;
        assert!(cluster.runtime_profile.validate(&cluster).is_err());
        cluster.role = ClusterRole::Runner;
        cluster.enabled = false;
        assert!(cluster.runtime_profile.validate(&cluster).is_err());
    }

    #[test]
    fn runner_profile_round_trip_preserves_module_plan() {
        let yaml = "enabled: true\nrole: runner\nruntime_profile: runner\nintake_routing:\n  enabled: true\n  mode: enforce\n";
        let cluster: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cluster.runtime_profile.validate(&cluster).is_ok());
        assert_eq!(cluster.runtime_profile, RuntimeProfile::Runner);
        assert!(!cluster.runtime_profile.modules().gateway);
        assert!(!cluster.runtime_profile.modules().hub_services);
        let value = serde_json::to_value(&cluster).unwrap();
        assert_eq!(value["role"], "runner");
        assert_eq!(value["runtime_profile"], "runner");
        let reloaded: ClusterConfig = serde_json::from_value(value).unwrap();
        assert_eq!(reloaded, cluster);
        for role in [ClusterRole::Hub, ClusterRole::Auto] {
            let mut cluster = ClusterConfig {
                enabled: true,
                role,
                runtime_profile: RuntimeProfile::Runner,
                ..Default::default()
            };
            cluster.intake_routing.enabled = true;
            assert!(cluster.runtime_profile.validate(&cluster).is_err());
        }
    }
}
