#!/bin/bash
git restore src/services/discord/health/relay_auto_heal.rs
sed -i 's/async fn redrive_actions_and_cap_alarm_continue_while_producer_is_vouched_4615()/async fn pg_redrive_actions_and_cap_alarm_continue_while_producer_is_vouched_4615()/' src/services/discord/health/relay_auto_heal.rs
