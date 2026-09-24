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

//! Message deduplication under real VSR replicas, crashes, restarts, view
//! changes and a hostile network. The properties every scenario checks:
//!
//! 1. no dedup key is committed twice (the window outlasts every run);
//! 2. every key of a request answered with success is committed (no loss);
//! 3. replicas hold the same committed messages;
//! 4. replicas hold the same dedup index for the same log.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::{Bytes, BytesMut};
use consensus::PartitionsHandle;
use iggy_binary_protocol::responses::messages::SendMessagesResponse;
use iggy_binary_protocol::{
    ReplyHeader, RoutedRequestHeader, WireDecode, WireOptions, encode_user_headers,
};
use iggy_common::{IggyDuration, IggyError, TopicCreateOptions};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::{PacketSimulatorOptions, ProcessId};

const HEADER: &str = "dedup-key";
const STRING_KIND: u8 = 2;
/// Far longer than any run here, so every repeat inside a run is a duplicate.
const WINDOW_MICROS: u64 = 3_600 * 1_000_000;

fn init_pool() {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
}

fn dedup_options() -> WireOptions {
    TopicCreateOptions {
        dedup_window: Some(IggyDuration::from(WINDOW_MICROS)),
        dedup_header: Some(HEADER.to_string()),
        ..TopicCreateOptions::default()
    }
    .to_wire()
    .expect("dedup options encode")
}

fn key_headers(key: &str) -> Bytes {
    let mut buffer = BytesMut::new();
    encode_user_headers(
        &[(STRING_KIND, HEADER.as_bytes(), STRING_KIND, key.as_bytes())],
        &mut buffer,
    );
    buffer.freeze()
}

/// The dedup key a committed message carries, if any.
fn key_of(user_headers: &[u8]) -> Option<String> {
    if user_headers.is_empty() || iggy_binary_protocol::validate_user_headers(user_headers).is_err()
    {
        return None;
    }
    iggy_binary_protocol::WireUserHeaderIterator::new(user_headers)
        .find(|entry| entry.key == HEADER.as_bytes())
        .map(|entry| String::from_utf8_lossy(entry.value).into_owned())
}

/// The partition `namespace` on `replica`, borrowed from the shard's plane.
macro_rules! partition {
    ($cluster:expr, $replica:expr) => {
        $cluster.sim.replicas[usize::from($replica)]
            .partition_shard($cluster.namespace)
            .plane
            .partitions()
            .get_mut_by_ns(&$cluster.namespace)
            .expect("partition materialised on a live replica")
    };
}

/// A replica's committed messages as `(offset, user headers)`.
type CommittedLog = Vec<(u64, Vec<u8>)>;

struct Cluster {
    sim: Simulator,
    namespace: IggyNamespace,
}

impl Cluster {
    fn new(replicas: u8, clients: &[u128], network: &PacketSimulatorOptions) -> Self {
        init_pool();
        let network = PacketSimulatorOptions {
            node_count: replicas,
            client_count: u8::try_from(clients.len()).expect("few clients"),
            ..network.clone()
        };
        let mut sim = Simulator::new(usize::from(replicas), clients.iter().copied(), network);
        let namespace = IggyNamespace::new(1, 1, 0);
        // Escape hatch for attributing a failure: the same run without the feature.
        if std::env::var("DEDUP_SIM_DISABLE").map_or(true, |value| value.is_empty()) {
            sim.set_topic_options(namespace, dedup_options());
        }
        sim.init_partition(namespace);
        Self { sim, namespace }
    }

    fn live_replicas(&self) -> Vec<u8> {
        (0..u8::try_from(self.sim.replicas.len()).expect("few replicas"))
            .filter(|index| !self.sim.is_crashed(*index))
            .collect()
    }

    /// The primary as the most advanced live replica sees it.
    fn primary(&self) -> u8 {
        let live = self.live_replicas();
        let view = live
            .iter()
            .map(|replica| partition!(self, *replica).consensus().view())
            .max()
            .expect("a live replica");
        partition!(self, live[0]).consensus().primary_index(view)
    }

    fn committed(&self, replica: u8) -> CommittedLog {
        futures::executor::block_on(partition!(self, replica).committed_message_headers())
    }

    fn step_until_reply(
        &mut self,
        client: u128,
        request: u64,
        max_steps: usize,
    ) -> Option<Message<ReplyHeader>> {
        for _ in 0..max_steps {
            if let Some(reply) =
                self.sim.step().into_iter().find(|reply| {
                    reply.header().client == client && reply.header().request == request
                })
            {
                return Some(reply);
            }
        }
        None
    }

    fn submit(&mut self, client: u128, target: u8, request: Message<RoutedRequestHeader>) {
        self.sim
            .submit_request(client, target, request.into_generic());
    }

    fn settle(&mut self, steps: usize) {
        for _ in 0..steps {
            self.sim.step();
        }
    }

    fn assert_invariants(&self, context: &str, acked_keys: &BTreeSet<String>) {
        let live = self.live_replicas();
        let logs: Vec<(u8, CommittedLog)> = live
            .iter()
            .map(|replica| (*replica, self.committed(*replica)))
            .collect();
        let longest = logs.iter().map(|(_, log)| log.len()).max().unwrap_or(0);
        for (replica, log) in &logs {
            let mut seen: HashMap<String, u64> = HashMap::new();
            for (offset, headers) in log {
                if let Some(key) = key_of(headers)
                    && let Some(first) = seen.insert(key.clone(), *offset)
                {
                    panic!(
                        "{context}: replica {replica} committed key {key} twice, at offsets {first} and {offset}"
                    );
                }
            }
            if log.len() < longest {
                // Lagging: its log is a prefix, checked for agreement below.
                continue;
            }
            for key in acked_keys {
                assert!(
                    seen.contains_key(key),
                    "{context}: replica {replica} lost key {key}, which was acknowledged"
                );
            }
        }
        Self::assert_prefixes_agree(context, &logs);
    }

    /// Whether every live replica reads a gap-free committed log that agrees
    /// with its peers. False only under the harness's frontier restore, whose
    /// restarts can leave a hole with or without deduplication.
    fn logs_are_sound(&self) -> bool {
        let logs: Vec<Vec<(u64, Vec<u8>)>> = self
            .live_replicas()
            .iter()
            .map(|replica| self.committed(*replica))
            .collect();
        let gap_free = logs
            .iter()
            .all(|log| log.windows(2).all(|pair| pair[1].0 == pair[0].0 + 1));
        let shortest = logs.iter().map(Vec::len).min().unwrap_or(0);
        // Batch timestamps too: a body that diverged under the same op keeps
        // its messages and differs only there.
        let stamps: Vec<Vec<(u64, u64)>> = self
            .live_replicas()
            .iter()
            .map(|replica| {
                futures::executor::block_on(
                    partition!(self, *replica).committed_message_timestamps(),
                )
            })
            .collect();
        let shortest_stamps = stamps.iter().map(Vec::len).min().unwrap_or(0);
        gap_free
            && logs
                .windows(2)
                .all(|pair| pair[0][..shortest] == pair[1][..shortest])
            && stamps
                .windows(2)
                .all(|pair| pair[0][..shortest_stamps] == pair[1][..shortest_stamps])
    }

    /// Replicas' committed messages agree on their common prefix. Not a dedup
    /// property, but the one the dedup properties stand on.
    fn assert_logs_agree(&self, context: &str) {
        let logs: Vec<(u8, CommittedLog)> = self
            .live_replicas()
            .iter()
            .map(|replica| (*replica, self.committed(*replica)))
            .collect();
        Self::assert_prefixes_agree(context, &logs);
    }

    fn assert_prefixes_agree(context: &str, logs: &[(u8, CommittedLog)]) {
        for (replica, log) in logs {
            for pair in log.windows(2) {
                assert_eq!(
                    pair[1].0,
                    pair[0].0 + 1,
                    "{context}: replica {replica} reads a gap in its committed offsets after {}",
                    pair[0].0
                );
            }
        }
        let shortest = logs
            .iter()
            .map(|(_, log)| log.len())
            .min()
            .expect("a live replica");
        for pair in logs.windows(2) {
            assert_eq!(
                pair[0].1[..shortest],
                pair[1].1[..shortest],
                "{context}: replicas {} and {} diverge in their committed messages",
                pair[0].0,
                pair[1].0
            );
        }
    }

    /// Once the group has quiesced, every replica holds the same log and so
    /// must hold the same index.
    fn assert_indexes_agree(&self, context: &str) {
        let live = self.live_replicas();
        let fingerprints: BTreeMap<u8, (u64, usize, u128)> = live
            .iter()
            .map(|replica| {
                let partition = partition!(self, *replica);
                (
                    *replica,
                    (
                        partition.consensus().commit_min(),
                        0,
                        partition.message_dedup_fingerprint(),
                    ),
                )
            })
            .collect();
        // A replica still catching up (a partition that never healed) holds a
        // shorter log; only replicas at the same commit must agree.
        let max_commit = fingerprints
            .values()
            .map(|(commit, _, _)| *commit)
            .max()
            .expect("a live replica");
        let at_max: BTreeMap<u8, (u64, usize, u128)> = fingerprints
            .iter()
            .filter(|(_, (commit, _, _))| *commit == max_commit)
            .map(|(replica, state)| (*replica, *state))
            .collect();
        let first = at_max.values().next().expect("a live replica");
        for (replica, state) in &at_max {
            assert_eq!(
                state, first,
                "{context}: replica {replica} holds a different dedup index ({fingerprints:?})"
            );
        }
    }
}

fn success(reply: &Message<ReplyHeader>) -> bool {
    reply.header().status == 0
}

#[test]
fn given_dedup_topic_when_keys_repeat_should_commit_each_key_once_on_every_replica() {
    let mut cluster = Cluster::new(3, &[1], &PacketSimulatorOptions::default());
    let client = SimClient::new(1);
    cluster.sim.register_client_with_primary(&client);

    let batches: Vec<Vec<(Bytes, Option<Bytes>)>> = vec![
        vec![
            (Bytes::from_static(b"a1"), Some(key_headers("a"))),
            (Bytes::from_static(b"b1"), Some(key_headers("b"))),
        ],
        // Repeats of `a` and `b`, a new key, a repeat inside the batch, and a
        // message with no key at all.
        vec![
            (Bytes::from_static(b"a2"), Some(key_headers("a"))),
            (Bytes::from_static(b"c1"), Some(key_headers("c"))),
            (Bytes::from_static(b"c2"), Some(key_headers("c"))),
            (Bytes::from_static(b"nokey"), None),
            (Bytes::from_static(b"b2"), Some(key_headers("b"))),
        ],
    ];
    for batch in batches {
        let request = client.send_messages_with_headers(cluster.namespace, &batch);
        let request_id = request.header().request;
        cluster.submit(1, 0, request);
        let reply = cluster.step_until_reply(1, request_id, 500).expect("reply");
        assert!(success(&reply));
    }
    cluster.settle(200);

    let expected: Vec<Option<String>> =
        vec![Some("a".into()), Some("b".into()), Some("c".into()), None];
    for replica in cluster.live_replicas() {
        let keys: Vec<Option<String>> = cluster
            .committed(replica)
            .iter()
            .map(|(_, headers)| key_of(headers))
            .collect();
        assert_eq!(keys, expected, "replica {replica}");
    }
    let acked: BTreeSet<String> = ["a", "b", "c"]
        .iter()
        .map(|key| (*key).to_string())
        .collect();
    cluster.assert_invariants("basic", &acked);
    cluster.assert_indexes_agree("basic");
}

#[test]
fn given_committed_originals_when_whole_batch_repeats_should_succeed_without_appending() {
    let mut cluster = Cluster::new(3, &[1, 2], &PacketSimulatorOptions::default());
    let producer = SimClient::new(1);
    let resender = SimClient::new(2);
    cluster.sim.register_client_with_primary(&producer);
    cluster.sim.register_client_with_primary(&resender);
    let batch = vec![
        (Bytes::from_static(b"x"), Some(key_headers("x"))),
        (Bytes::from_static(b"y"), Some(key_headers("y"))),
    ];
    let original = producer.send_messages_with_headers(cluster.namespace, &batch);
    let original_id = original.header().request;
    cluster.submit(1, 0, original);
    assert!(success(
        &cluster
            .step_until_reply(1, original_id, 500)
            .expect("reply")
    ));
    cluster.settle(100);
    let before = cluster.committed(0).len();

    // A different client (new session: request-level dedup cannot help) sends
    // the same events.
    let repeat = resender.send_messages_with_headers(cluster.namespace, &batch);
    let repeat_id = repeat.header().request;
    cluster.submit(2, 0, repeat);
    let reply = cluster.step_until_reply(2, repeat_id, 500).expect("reply");
    assert!(
        success(&reply),
        "a fully duplicate batch of committed events is a success"
    );
    // A success every SDK reads as one: the Go SDK rejects a status-0
    // `SendMessages` reply without its confirmation section (the shape of a
    // send that wrote nothing on a dead session), so an empty body would turn
    // an absorbed resend into an error its producer retries forever.
    let body = &reply.as_slice()[size_of::<ReplyHeader>()..reply.header().size as usize];
    let (confirmations, _) = SendMessagesResponse::decode(body)
        .expect("a duplicate send is answered with a confirmation section");
    assert!(
        confirmations.confirmations.is_empty(),
        "nothing was appended, so no placement to confirm"
    );
    cluster.settle(100);
    for replica in cluster.live_replicas() {
        assert_eq!(
            cluster.committed(replica).len(),
            before,
            "replica {replica}"
        );
    }
    let dropped: u64 = cluster.sim.replicas[0].shards[0]
        .metrics()
        .partition_message_dedup_dropped_value();
    assert!(dropped >= 2, "the drop must be metered, saw {dropped}");
}

#[test]
fn given_uncommitted_original_when_whole_batch_repeats_should_answer_transient_then_succeed() {
    // The duplicate lands while its original is still in the pipeline. A
    // success here would be a lie if a view change truncated the original,
    // so it must be refused transiently, and succeed once the original commits.
    let mut cluster = Cluster::new(3, &[1, 2], &PacketSimulatorOptions::default());
    let producer = SimClient::new(1);
    let resender = SimClient::new(2);
    cluster.sim.register_client_with_primary(&producer);
    cluster.sim.register_client_with_primary(&resender);
    let batch = vec![(Bytes::from_static(b"p"), Some(key_headers("pending")))];

    let original = producer.send_messages_with_headers(cluster.namespace, &batch);
    let original_id = original.header().request;
    let repeat = resender.send_messages_with_headers(cluster.namespace, &batch);
    let repeat_id = repeat.header().request;
    let repeat_again = repeat.deep_copy();
    // Cut the backups off so the original cannot reach quorum while the
    // duplicate is screened.
    cluster.sim.network.process_disable(ProcessId::Replica(1));
    cluster.sim.network.process_disable(ProcessId::Replica(2));
    cluster.submit(1, 0, original);
    cluster.settle(50);
    cluster.submit(2, 0, repeat);

    let repeat_reply = cluster
        .step_until_reply(2, repeat_id, 300)
        .expect("the duplicate is answered at admission, without quorum");
    assert_eq!(
        repeat_reply.header().status,
        IggyError::TransientNotCommitted.as_code(),
        "a duplicate of an uncommitted message must not claim success"
    );
    cluster.sim.network.process_enable(ProcessId::Replica(1));
    cluster.sim.network.process_enable(ProcessId::Replica(2));

    let mut original_reply = None;
    for _ in 0..2_000 {
        if let Some(reply) = cluster
            .sim
            .step()
            .into_iter()
            .find(|reply| reply.header().client == 1 && reply.header().request == original_id)
        {
            original_reply = Some(reply);
            break;
        }
    }
    assert!(success(
        &original_reply.expect("original commits once quorum returns")
    ));

    cluster.submit(2, 0, repeat_again);
    let retried = cluster
        .step_until_reply(2, repeat_id, 500)
        .expect("retry reply");
    assert!(success(&retried));
    cluster.settle(100);
    let acked: BTreeSet<String> = std::iter::once("pending".to_string()).collect();
    cluster.assert_invariants("pending", &acked);
    for replica in cluster.live_replicas() {
        assert_eq!(cluster.committed(replica).len(), 1, "replica {replica}");
    }
}

#[test]
fn given_primary_crash_when_new_producer_resends_should_be_absorbed_by_the_inherited_index() {
    let mut cluster = Cluster::new(3, &[1, 2], &PacketSimulatorOptions::default());
    let producer = SimClient::new(1);
    cluster.sim.register_client_with_primary(&producer);
    let batch: Vec<(Bytes, Option<Bytes>)> = (0..5)
        .map(|index| {
            (
                Bytes::from(format!("event-{index}")),
                Some(key_headers(&format!("event-{index}"))),
            )
        })
        .collect();
    let request = producer.send_messages_with_headers(cluster.namespace, &batch);
    let request_id = request.header().request;
    cluster.submit(1, 0, request);
    assert!(success(
        &cluster.step_until_reply(1, request_id, 500).expect("reply")
    ));
    cluster.settle(100);

    cluster.sim.replica_crash(0);
    cluster.settle(1_500);
    let primary = cluster.primary();
    assert_ne!(primary, 0, "a survivor must have taken over");

    // A restarted producer: new client, new session, same events plus one.
    let restarted = SimClient::new(2);
    cluster.sim.register_client_via(&restarted, primary);
    let mut resend = batch;
    resend.push((Bytes::from_static(b"new"), Some(key_headers("event-new"))));
    let request = restarted.send_messages_with_headers(cluster.namespace, &resend);
    let request_id = request.header().request;
    cluster.submit(2, primary, request);
    assert!(success(
        &cluster
            .step_until_reply(2, request_id, 1_000)
            .expect("reply")
    ));
    cluster.settle(200);

    let mut acked: BTreeSet<String> = (0..5).map(|index| format!("event-{index}")).collect();
    acked.insert("event-new".to_string());
    cluster.assert_invariants("failover", &acked);
    for replica in cluster.live_replicas() {
        assert_eq!(cluster.committed(replica).len(), 6, "replica {replica}");
    }
}

#[test]
fn given_backup_restart_should_rebuild_the_same_index_as_its_peers() {
    let mut cluster = Cluster::new(3, &[1], &PacketSimulatorOptions::default());
    let producer = SimClient::new(1);
    cluster.sim.register_client_with_primary(&producer);
    let mut acked = BTreeSet::new();
    for round in 0..6 {
        let batch: Vec<(Bytes, Option<Bytes>)> = (0..4)
            .map(|index| {
                let key = format!("k-{round}-{index}");
                acked.insert(key.clone());
                (Bytes::from(key.clone()), Some(key_headers(&key)))
            })
            .collect();
        let request = producer.send_messages_with_headers(cluster.namespace, &batch);
        let request_id = request.header().request;
        cluster.submit(1, 0, request);
        assert!(success(
            &cluster.step_until_reply(1, request_id, 500).expect("reply")
        ));
    }
    cluster.settle(200);
    cluster.assert_indexes_agree("before restart");

    cluster.sim.replica_crash(2);
    cluster.settle(50);
    cluster.sim.replica_restart(2);
    cluster.settle(3_000);
    cluster.assert_invariants("after restart", &acked);
    cluster.assert_indexes_agree("after restart");
}

#[test]
fn given_restart_with_retained_log_should_rebuild_the_index_without_any_repair() {
    // With the frontier restored the replica needs no repair: nothing is
    // re-appended, so only the rebuild can bring its index back.
    let mut cluster = Cluster::new(3, &[1], &PacketSimulatorOptions::default());
    cluster.sim.set_restore_partition_frontier(true);
    let producer = SimClient::new(1);
    cluster.sim.register_client_with_primary(&producer);
    let mut acked = BTreeSet::new();
    for round in 0..4 {
        let batch: Vec<(Bytes, Option<Bytes>)> = (0..3)
            .map(|index| {
                let key = format!("r-{round}-{index}");
                acked.insert(key.clone());
                (Bytes::from(key.clone()), Some(key_headers(&key)))
            })
            .collect();
        let request = producer.send_messages_with_headers(cluster.namespace, &batch);
        let request_id = request.header().request;
        cluster.submit(1, 0, request);
        assert!(success(
            &cluster.step_until_reply(1, request_id, 500).expect("reply")
        ));
    }
    cluster.settle(200);
    let before = partition!(cluster, 1u8).message_dedup_len();
    assert_eq!(before, 12);
    cluster.sim.replica_crash(1);
    cluster.settle(20);
    cluster.sim.replica_restart(1);
    let rebuilt = partition!(cluster, 1u8).message_dedup_len();
    assert_eq!(rebuilt, before, "rebuild right after restart");
    cluster.settle(1_000);
    cluster.assert_indexes_agree("after restart");
    cluster.assert_invariants("after restart", &acked);
}

/// A fuzz producer's request awaiting its reply.
struct InFlight {
    request: Message<RoutedRequestHeader>,
    keys: Vec<String>,
    sent_at: usize,
}

/// Replica faults a fuzz run injects.
#[derive(Clone, Copy, Debug)]
enum FaultMode {
    /// Three replicas, one at a time crashed and later restarted. Restart uses
    /// the harness's frontier restore: without it a restarted in-memory
    /// partition trips the sequential-advance assert, with or without dedup
    /// (see `Simulator::set_restore_partition_frontier`).
    CrashRestart,
    /// Five replicas, up to two crashed for good: view changes over survivors
    /// with production restart semantics never modelled wrong.
    CrashOnly,
}

/// One seeded run of the dedup fuzz: several producers resend events through
/// retries, new sessions and concurrent duplicates, while replicas crash (and
/// maybe restart) under a seed-drawn hostile network.
#[allow(clippy::too_many_lines)]
fn dedup_fuzz(seed: u64, steps: usize, mode: FaultMode) -> bool {
    const CLIENTS: [u128; 3] = [1, 2, 3];
    const KEYSPACE: u32 = 48;
    const REPLY_TIMEOUT_STEPS: usize = 400;

    let replicas: u8 = match mode {
        FaultMode::CrashRestart => 3,
        FaultMode::CrashOnly => 5,
    };
    let mut network = PacketSimulatorOptions::swarm(seed);
    network.seed = seed;
    let mut cluster = Cluster::new(replicas, &CLIENTS, &network);
    if matches!(mode, FaultMode::CrashRestart) {
        cluster.sim.set_restore_partition_frontier(true);
    }
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(seed ^ 0x0dd5_eed0);
    // Ordered: iteration order drives the trajectory, and a seed must replay.
    let clients: BTreeMap<u128, SimClient> = CLIENTS
        .iter()
        .map(|id| (*id, SimClient::new(*id)))
        .collect();
    for client in clients.values() {
        cluster.sim.register_client_with_primary(client);
    }

    let mut in_flight: HashMap<u128, InFlight> = HashMap::new();
    let mut acked: BTreeSet<String> = BTreeSet::new();
    let mut recent: Vec<Vec<(Bytes, Option<Bytes>)>> = Vec::new();
    let mut crashed_at: Option<(u8, usize)> = None;
    let mut crashed_for_good: Vec<u8> = Vec::new();
    let mut successes = 0u64;

    for step in 0..steps {
        // Faults, always leaving a quorum alive.
        match mode {
            FaultMode::CrashRestart => match crashed_at {
                None if rng.random_range(0..1_000u32) < 2 => {
                    let victim = rng.random_range(0..replicas);
                    cluster.sim.replica_crash(victim);
                    crashed_at = Some((victim, step));
                }
                Some((victim, at)) if step > at + 400 && rng.random_range(0..100u32) < 5 => {
                    cluster.sim.replica_restart(victim);
                    crashed_at = None;
                }
                _ => {}
            },
            FaultMode::CrashOnly => {
                if crashed_for_good.len() < 2 && rng.random_range(0..1_000u32) < 1 {
                    let victim = rng.random_range(0..replicas);
                    if !crashed_for_good.contains(&victim) {
                        cluster.sim.replica_crash(victim);
                        crashed_for_good.push(victim);
                    }
                }
            }
        }

        for (client_id, client) in &clients {
            let resend = in_flight
                .get(client_id)
                .is_some_and(|pending| step > pending.sent_at + REPLY_TIMEOUT_STEPS);
            if in_flight.contains_key(client_id) && !resend {
                continue;
            }
            if resend && rng.random_range(0..5u32) == 0 {
                // The producer died before retrying: its events may never
                // commit. Another producer's copy of them must not have been
                // dropped against this one, or they are lost.
                in_flight.remove(client_id);
                continue;
            }
            if resend {
                // The SDK retry: the same request, to whoever is primary now.
                let pending = in_flight.get_mut(client_id).expect("checked");
                pending.sent_at = step;
                let copy = pending.request.deep_copy();
                let primary = cluster.primary();
                cluster.submit(*client_id, primary, copy);
                continue;
            }
            if rng.random_range(0..10u32) < 3 {
                continue;
            }
            // A fresh request: a brand-new batch, or a replay of a recent one
            // as a restarted or second producer would send it.
            let batch: Vec<(Bytes, Option<Bytes>)> =
                if !recent.is_empty() && rng.random_range(0..3u32) == 0 {
                    recent[rng.random_range(0..recent.len())].clone()
                } else {
                    (0..rng.random_range(1..=4u32))
                        .map(|_| {
                            if rng.random_range(0..10u32) == 0 {
                                (Bytes::from_static(b"unkeyed"), None)
                            } else {
                                let key = format!("k{}", rng.random_range(0..KEYSPACE));
                                (Bytes::from(key.clone()), Some(key_headers(&key)))
                            }
                        })
                        .collect()
                };
            recent.push(batch.clone());
            if recent.len() > 16 {
                recent.remove(0);
            }
            let keys = batch
                .iter()
                .filter_map(|(_, headers)| headers.as_deref().and_then(key_of))
                .collect();
            let request = client.send_messages_with_headers(cluster.namespace, &batch);
            let copy = request.deep_copy();
            in_flight.insert(
                *client_id,
                InFlight {
                    request,
                    keys,
                    sent_at: step,
                },
            );
            let primary = cluster.primary();
            cluster.submit(*client_id, primary, copy);
        }

        for reply in cluster.sim.step() {
            let client_id = reply.header().client;
            let Some(pending) = in_flight.get(&client_id) else {
                continue;
            };
            if reply.header().request != pending.request.header().request {
                continue;
            }
            if success(&reply) {
                acked.extend(pending.keys.iter().cloned());
                successes += 1;
                in_flight.remove(&client_id);
            } else {
                // Transient refusals are retried as the SDK would.
                if let Some(pending) = in_flight.get_mut(&client_id) {
                    pending.sent_at = 0;
                }
            }
        }
    }

    if let Some((victim, _)) = crashed_at {
        cluster.sim.replica_restart(victim);
    }
    cluster.settle(8_000);
    let context = format!("seed {seed} {mode:?}");
    if std::env::var("DEDUP_SIM_DISABLE").is_ok_and(|value| !value.is_empty()) {
        cluster.assert_logs_agree(&context);
        return true;
    }
    if successes == 0 {
        // The drawn network never let a request through: nothing to judge.
        return false;
    }
    if matches!(mode, FaultMode::CrashRestart) && !cluster.logs_are_sound() {
        // Inconclusive: the frontier-restoring restart left the log itself
        // broken, which the dedup properties cannot be judged against.
        return false;
    }
    cluster.assert_invariants(&context, &acked);
    cluster.assert_indexes_agree(&context);
    true
}

#[test]
#[ignore = "debug: replays one seed from DEDUP_FUZZ_SEED"]
fn dedup_fuzz_one_seed() {
    let seed = std::env::var("DEDUP_FUZZ_SEED").unwrap().parse().unwrap();
    let mode = if std::env::var("DEDUP_FUZZ_CRASH_ONLY").is_ok_and(|value| !value.is_empty()) {
        FaultMode::CrashOnly
    } else {
        FaultMode::CrashRestart
    };
    let steps = std::env::var("DEDUP_FUZZ_STEPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    let conclusive = dedup_fuzz(seed, steps, mode);
    eprintln!("conclusive: {conclusive}");
}

#[test]
fn dedup_fuzz_fixed_seeds_crash_restart() {
    let conclusive = (1..=24u64)
        .filter(|seed| dedup_fuzz(*seed, 6_000, FaultMode::CrashRestart))
        .count();
    // Most runs must be judgeable, or this test would pass by skipping.
    assert!(
        conclusive >= 8,
        "only {conclusive} of 24 runs were conclusive"
    );
}

#[test]
fn dedup_fuzz_fixed_seeds_crash_only() {
    for seed in [1, 2, 3, 7, 42, 1_337, 9_001, 65_537] {
        assert!(dedup_fuzz(seed, 6_000, FaultMode::CrashOnly));
    }
}

/// Seeds that caught real defects or mutants, replayed on every run. 30104
/// loses an acknowledged event when the primary drops a message against an
/// uncommitted occurrence.
#[test]
fn dedup_fuzz_regression_seeds() {
    assert!(dedup_fuzz(30_104, 10_000, FaultMode::CrashOnly));
}

/// A longer campaign; run with `cargo test -p simulator --release -- --ignored`
/// and `DEDUP_FUZZ_SEEDS=<count>`.
#[test]
#[ignore = "long-running campaign"]
fn dedup_fuzz_campaign() {
    let seeds: u64 = std::env::var("DEDUP_FUZZ_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let base: u64 = std::env::var("DEDUP_FUZZ_BASE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    let modes: Vec<FaultMode> = match std::env::var("DEDUP_FUZZ_MODE").as_deref() {
        Ok("crash-restart") => vec![FaultMode::CrashRestart],
        Ok("crash-only") => vec![FaultMode::CrashOnly],
        _ => vec![FaultMode::CrashRestart, FaultMode::CrashOnly],
    };
    let keep_going = std::env::var_os("DEDUP_FUZZ_KEEP_GOING").is_some();
    let mut failed = Vec::new();
    let mut conclusive = 0u64;
    let mut inconclusive = 0u64;
    for seed in base..base + seeds {
        for mode in &modes {
            match std::panic::catch_unwind(|| dedup_fuzz(seed, 10_000, *mode)) {
                Ok(true) => conclusive += 1,
                Ok(false) => inconclusive += 1,
                Err(_) => {
                    failed.push((seed, *mode));
                    assert!(keep_going, "seed {seed} {mode:?} failed");
                }
            }
        }
    }
    eprintln!(
        "campaign: {conclusive} conclusive, {inconclusive} inconclusive, {} failed",
        failed.len()
    );
    assert!(failed.is_empty(), "failed runs: {failed:?}");
}
