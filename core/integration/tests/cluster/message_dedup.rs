// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Message deduplication across real server processes: the index a node
//! rebuilds from its own segments after a restart, the one it rebuilds from a
//! state-transfer install after losing its disk, and the one a backup inherits
//! when it takes over as primary must all stop a resent event from being
//! appended again.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use tokio::time::{Instant, sleep};

const STREAM_NAME: &str = "dedup-stream";
const TOPIC_NAME: &str = "dedup-topic";
const PARTITION_ID: u32 = 0;
const HEADER: &str = "dedup-key";
/// Room for a failover to settle before a send or sign-in is accepted.
const OPERATION_BUDGET: Duration = Duration::from_secs(30);
const MARKER_BUDGET: Duration = Duration::from_secs(90);
const RETRY_PAUSE: Duration = Duration::from_millis(200);

fn keyed_message(key: &str) -> IggyMessage {
    let headers = BTreeMap::from([(
        HeaderKey::from_str(HEADER).expect("header key"),
        HeaderValue::from_str(key).expect("header value"),
    )]);
    IggyMessage::builder()
        .payload(format!("payload-{key}").into())
        .user_headers(headers)
        .build()
        .expect("build message")
}

async fn connect_any(harness: &TestHarness, nodes: &[usize]) -> Option<IggyClient> {
    for &node in nodes {
        if let Ok(builder) = harness.node(node).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
        {
            return Some(client);
        }
    }
    None
}

async fn create_dedup_topic(client: &IggyClient, durability: Durability) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).expect("stream identifier"),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                // Every commit reaches the segment files, so a restart has to
                // rebuild the index from disk rather than from a replayed log.
                messages_required_to_save: Some(1),
                durability,
                dedup_window: Some(IggyDuration::from_str("1h").expect("window")),
                dedup_header: Some(HEADER.to_string()),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create dedup topic");
}

/// Send `keys` as one batch, retrying through failover until it is accepted.
async fn send_keys(harness: &TestHarness, live: &[usize], keys: &[String]) {
    let deadline = Instant::now() + OPERATION_BUDGET;
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let last_error = match connect_any(harness, live).await {
            Some(client) => {
                let mut messages: Vec<IggyMessage> =
                    keys.iter().map(|key| keyed_message(key)).collect();
                match client
                    .send_messages(
                        &Identifier::named(STREAM_NAME).expect("stream identifier"),
                        &Identifier::named(TOPIC_NAME).expect("topic identifier"),
                        &Partitioning::partition_id(PARTITION_ID),
                        &mut messages,
                    )
                    .await
                {
                    Ok(_) => return,
                    Err(error) => error.to_string(),
                }
            }
            None => format!("no connection to {live:?}"),
        };
        assert!(
            Instant::now() < deadline,
            "a batch of {} keys was not accepted within {OPERATION_BUDGET:?} \
             ({attempts} attempts, last error: {last_error})",
            keys.len()
        );
        sleep(RETRY_PAUSE).await;
    }
}

/// Every committed message's dedup key, in offset order.
async fn committed_keys(client: &IggyClient) -> Vec<String> {
    let mut keys = Vec::new();
    let mut offset = 0u64;
    loop {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).expect("stream identifier"),
                &Identifier::named(TOPIC_NAME).expect("topic identifier"),
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                1_000,
                false,
            )
            .await
            .expect("poll committed messages");
        if polled.messages.is_empty() {
            return keys;
        }
        for message in &polled.messages {
            offset = message.header.offset + 1;
            if let Some(headers) = message.user_headers_map().expect("user headers parse")
                && let Some(value) = headers.get(&HeaderKey::from_str(HEADER).expect("header key"))
            {
                keys.push(value.as_str().expect("string key").to_string());
            }
        }
    }
}

fn assert_each_key_once(keys: &[String], expected: usize, context: &str) {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for key in keys {
        *counts.entry(key).or_default() += 1;
    }
    let repeated: Vec<(&&str, &usize)> = counts.iter().filter(|(_, count)| **count > 1).collect();
    assert!(
        repeated.is_empty(),
        "{context}: keys appended more than once: {repeated:?}"
    );
    assert_eq!(
        counts.len(),
        expected,
        "{context}: every sent key must be committed once"
    );
}

fn batch(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index}"))
        .collect()
}

const INSTALL_MARKER: &str = "partition state transfer installed";
/// More committed history than the shrunken repair ring holds (see the
/// harness `server(...)` overrides), so a node that lost track of it cannot
/// get it back op by op from its peers and has to rely on its own segments or
/// a state-transfer install. That is exactly when the index rebuild is the
/// only thing standing between a resend and a duplicate.
const HISTORY_KEYS: usize = 150;

/// One message per send so every event is its own op and flush.
async fn seed_history(client: &IggyClient, keys: &[String]) {
    for key in keys {
        let mut messages = vec![keyed_message(key)];
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).expect("stream identifier"),
                &Identifier::named(TOPIC_NAME).expect("topic identifier"),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .expect("seed send");
    }
}

async fn await_marker(harness: &TestHarness, node: usize, marker: &str) {
    let deadline = Instant::now() + MARKER_BUDGET;
    while !harness.node(node).stdout_contains(marker) {
        assert!(
            Instant::now() < deadline,
            "node {node} never logged {marker:?}"
        );
        sleep(RETRY_PAUSE).await;
    }
}

/// Make `target` the partition primary by failing over whoever leads,
/// restarting each deposed leader with its data so a quorum always remains.
async fn make_leader(harness: &mut TestHarness, target: usize) {
    for _ in 0..12 {
        let leader = disk::leader_node_index(harness).await;
        if leader == target {
            return;
        }
        harness.kill_node(leader).expect("SIGKILL the leader");
        harness
            .restart_node(leader)
            .expect("restart it with its data");
        sleep(Duration::from_secs(2)).await;
    }
    panic!("node {target} never became the primary");
}

/// Wait until `node` answers a signed-in request, so what follows measures
/// deduplication rather than the cluster settling after a failover.
async fn await_ready(harness: &TestHarness, node: usize) {
    let deadline = Instant::now() + OPERATION_BUDGET;
    loop {
        if let Some(client) = connect_any(harness, &[node]).await
            && client.ping().await.is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "node {node} never became ready");
        sleep(RETRY_PAUSE).await;
    }
}

/// Resend every event (plus new ones) straight to `target`, then check that
/// the log holds each key exactly once.
async fn resend_through(harness: &TestHarness, target: usize, history: &[String], tag: &str) {
    let fresh = batch(tag, 10);
    let mut resend = history.to_vec();
    resend.extend(fresh.iter().cloned());
    for chunk in resend.chunks(25) {
        send_keys(harness, &[target], chunk).await;
    }
    let client = connect_any(harness, &[target])
        .await
        .expect("target is live");
    assert_each_key_once(&committed_keys(&client).await, resend.len(), tag);
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64"
    )
)]
async fn given_dedup_topic_when_the_primary_restarts_from_disk_should_never_append_a_key_twice(
    harness: &mut TestHarness,
) {
    let client = harness.root_client_for_node(0).await.expect("root client");
    create_dedup_topic(&client, Durability::Replicated).await;
    let history = batch("history", HISTORY_KEYS);
    seed_history(&client, &history).await;
    // Kept connected: a disconnect commits a Logout the restarted node's
    // metadata would have to replay.
    let _seed_client = client;

    // The primary dies and comes back from its own disk; the survivors carry
    // on, and whoever leads afterwards decides the resend.
    let leader = disk::leader_node_index(harness).await;
    harness.kill_node(leader).expect("SIGKILL the primary");
    harness
        .restart_node(leader)
        .expect("restart it with its data");
    await_ready(harness, leader).await;
    let now_leading = disk::leader_node_index(harness).await;
    resend_through(harness, now_leading, &history, "disk-restart").await;
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64"
    )
)]
async fn given_persisted_dedup_topic_when_a_node_restarts_from_disk_and_leads_should_never_append_a_key_twice(
    harness: &mut TestHarness,
) {
    let client = harness.root_client_for_node(0).await.expect("root client");
    create_dedup_topic(&client, Durability::Persisted).await;
    let history = batch("history", HISTORY_KEYS);
    seed_history(&client, &history).await;
    let _seed_client = client;

    let target = 2;
    harness.kill_node(target).expect("SIGKILL the target");
    harness
        .restart_node(target)
        .expect("restart it with its data");
    sleep(Duration::from_secs(2)).await;
    make_leader(harness, target).await;
    await_ready(harness, target).await;
    resend_through(harness, target, &history, "persisted-restart").await;
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64"
    )
)]
async fn given_persisted_dedup_topic_when_a_wiped_node_rejoins_by_state_transfer_and_leads_should_never_append_a_key_twice(
    harness: &mut TestHarness,
) {
    let client = harness.root_client_for_node(0).await.expect("root client");
    create_dedup_topic(&client, Durability::Persisted).await;
    let history = batch("history", HISTORY_KEYS);
    seed_history(&client, &history).await;
    let _seed_client = client;

    // No local history at all and more of it than the ring holds: the node
    // can only come back through an install, then must rebuild from it.
    let target = 2;
    harness
        .restart_node_from_clean_slate(target)
        .expect("clean-slate restart");
    await_marker(harness, target, INSTALL_MARKER).await;
    make_leader(harness, target).await;
    await_ready(harness, target).await;
    resend_through(harness, target, &history, "state-transfer").await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_dedup_topic_should_report_its_policy_and_count_drops(harness: &mut TestHarness) {
    let client = harness.tcp_root_client().await.expect("root client");
    create_dedup_topic(&client, Durability::Replicated).await;
    let topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).expect("stream identifier"),
            &Identifier::named(TOPIC_NAME).expect("topic identifier"),
        )
        .await
        .expect("get topic")
        .expect("topic exists");
    let options = format!("{:?}", topic.options);
    assert!(
        options.contains("dedup_window") && options.contains("dedup_header"),
        "GetTopic must report the dedup policy: {options}"
    );
    drop(client);

    let keys = batch("metered", 5);
    send_keys(harness, &[0, 1, 2], &keys).await;
    send_keys(harness, &[0, 1, 2], &keys).await;
    let client = connect_any(harness, &[0, 1, 2]).await.expect("a live node");
    assert_each_key_once(&committed_keys(&client).await, keys.len(), "metered");
}
