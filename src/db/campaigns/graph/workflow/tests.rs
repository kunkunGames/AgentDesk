use super::*;
use serde_json::json;

fn frontier() -> Frontier {
    let goal:Goal=serde_json::from_value(json!({"objective":"Synchronize changed targets",
        "success":["all configured targets inspected","eligible changes synchronized or fresh no-work proved"],
        "constraints":["preserve prior effects"],"repo":"test/repo","planner":"planner","verifier":"verifier",
        "agents":["planner","verifier"],"budget":{"max_dispatches":12,"max_elapsed_secs":600,
        "node_timeout_secs":120,"max_evolutions":2}})).unwrap();
    let node = |id: &str, agent: &str, kind: &str, dependencies: Vec<&str>, inputs: Vec<Value>| {
        json!({
        "id":id,"task_key":format!("scope:input:{id}"),"title":id,"agent":agent,"kind":kind,
        "effect":"artifact_only","details":"Execute the declared bounded business step",
        "acceptance":["actual evidence"],"dependencies":dependencies,"inputs":inputs,
        "outputs":[{"path":format!("{id}.json"),"format":"json"}]})
    };
    let input = |id: &str| json!({"dependency":id,"source":format!("{id}.json"),"destination":format!("input/{id}.json")});
    let graph:Graph=serde_json::from_value(json!({"nodes":[
        node("inspect","planner","inspect",vec![],vec![]),
        node("check","verifier","verify",vec!["inspect"],vec![input("inspect")]),
        node("sync","planner","execute",vec!["inspect","check"],vec![input("inspect"),input("check")]),
        node("final","verifier","verify",vec!["sync"],vec![input("sync")])]})).unwrap();
    graph.validate(&goal).unwrap();
    let tasks=graph.nodes[..2].iter().map(|node|json!({"task_key":node.task_key,
        "contract_digest":contract_digest(node).unwrap(),"accepted":true,"observation":{"outcome":"accepted"},
        "artifact_manifest":{"bundle":"a".repeat(64),"files":{format!("{}.json",node.id):"b".repeat(64)}}})).collect();
    Frontier {
        campaign_id: "scope".into(),
        goal_digest: contract_digest(&goal).unwrap(),
        goal,
        version: 1,
        graph_digest: contract_digest(&graph).unwrap(),
        graph,
        tasks,
        failures: vec![],
        unmet_goal_assessments: vec![],
    }
}

fn decision(frontier: &Frontier, action: Action) -> Decision {
    Decision {
        snapshot_digest: frontier.digest().unwrap(),
        action,
        reason: "Current independent inspection proves no eligible changes".into(),
        failure_id: None,
        wake_after_secs: None,
    }
}

fn assessment(frontier: &Frontier) -> (GoalValidation, Value) {
    let proof = Criterion {
        index: 0,
        met: true,
        observation: 0,
        task_key: "scope:input:check".into(),
        path: "check.json".into(),
        sha256: "b".repeat(64),
        evidence: "Current configured targets match the inspected state".into(),
    };
    let mut second = proof.clone();
    second.index = 1;
    (
        GoalValidation {
            snapshot_digest: frontier.digest().unwrap(),
            approved: true,
            criteria: vec![proof, second],
            reason: "Current complete target inventory independently proves no-work".into(),
        },
        json!({"campaign_receipt":{"observations":[{"command":"read and verify inspection","exit_code":0,"evidence":"current configured targets checked"}]}}),
    )
}

#[test]
fn progress_can_prune_unpublished_suffix_without_a_fabricated_failure() {
    let frontier = frontier();
    let mut next = frontier.graph.clone();
    next.nodes.truncate(2);
    frontier
        .validate_decision(&decision(&frontier, Action::Revise), Some(&next))
        .unwrap();
    let mut changed = next.clone();
    changed.nodes[0].details = "Different published contract".into();
    assert!(
        frontier
            .validate_decision(&decision(&frontier, Action::Revise), Some(&changed))
            .is_err()
    );
    let mut published = frontier.clone();
    let node = &published.graph.nodes[2];
    published.tasks.push(json!({"task_key":node.task_key,"contract_digest":contract_digest(node).unwrap(),"accepted":false,"observation":null}));
    assert!(
        published
            .validate_decision(&decision(&published, Action::Revise), Some(&next))
            .is_err()
    );
}

#[test]
fn new_result_and_negative_goal_evidence_change_the_frontier_but_reads_do_not() {
    let mut frontier = frontier();
    let initial = frontier.digest().unwrap();
    assert_eq!(initial, frontier.clone().digest().unwrap());
    frontier.tasks[0]["result_digest"] = json!("c".repeat(64));
    assert_ne!(initial, frontier.digest().unwrap());
    let before = frontier.digest().unwrap();
    frontier
        .unmet_goal_assessments
        .push(json!({"criterion":1,"met":false}));
    assert_ne!(before, frontier.digest().unwrap());
}

#[test]
fn completion_covers_each_goal_criterion_and_exact_accepted_verification_output() {
    let frontier = frontier();
    let (good, result) = assessment(&frontier);
    frontier.validate_goal(&good, &result).unwrap();
    for bad in 0..6 {
        let mut validation = good.clone();
        match bad {
            0 => {
                validation.criteria.pop();
            }
            1 => validation.criteria[1].index = 0,
            2 => validation.criteria[1].sha256 = "c".repeat(64),
            3 => validation.criteria[1].task_key = "scope:input:inspect".into(),
            4 => validation.criteria[1].observation = 1,
            _ => validation.snapshot_digest = "d".repeat(64),
        }
        assert!(
            frontier.validate_goal(&validation, &result).is_err(),
            "case {bad}"
        );
    }
}

#[test]
fn unmet_goal_is_a_valid_assessment_and_never_completion_authority() {
    let frontier = frontier();
    let (mut assessment, result) = assessment(&frontier);
    assessment.approved = false;
    assessment.criteria[1].met = false;
    frontier
        .validate_goal_assessment(&assessment, &result)
        .unwrap();
    assert!(frontier.validate_goal(&assessment, &result).is_err());
}

#[test]
fn incomplete_independent_verification_and_unsettled_work_block_completion() {
    let mut frontier = frontier();
    frontier.tasks.pop();
    assert!(frontier.ready_verification());
    assert!(
        frontier
            .validate_decision(&decision(&frontier, Action::Complete), None)
            .is_err()
    );
    let node = &frontier.graph.nodes[1];
    frontier
        .tasks
        .push(json!({"task_key":node.task_key,"accepted":false,"observation":null}));
    assert!(frontier.running());
    assert!(
        frontier
            .validate_decision(&decision(&frontier, Action::Complete), None)
            .is_err()
    );
}

#[test]
fn wait_requires_a_bounded_wake_and_continue_requires_actual_ready_work() {
    let frontier = frontier();
    let mut wait = decision(&frontier, Action::Wait);
    assert!(frontier.validate_decision(&wait, None).is_err());
    wait.wake_after_secs = Some(3601);
    assert!(frontier.validate_decision(&wait, None).is_err());
    wait.wake_after_secs = Some(30);
    frontier.validate_decision(&wait, None).unwrap();
    frontier
        .validate_decision(&decision(&frontier, Action::Continue), None)
        .unwrap();
    let mut done = frontier.clone();
    done.graph.nodes.truncate(2);
    done.graph_digest = contract_digest(&done.graph).unwrap();
    assert!(
        done.validate_decision(&decision(&done, Action::Continue), None)
            .is_err()
    );
}
