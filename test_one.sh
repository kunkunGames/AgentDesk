#!/bin/bash
cargo test --package agentdesk redrive_actions_and_cap_alarm_continue_while_producer_is_vouched_4615 -- --nocapture > test_output.log 2>&1
