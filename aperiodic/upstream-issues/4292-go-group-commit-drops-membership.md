<!-- markdownlint-disable MD013 -->

# Upstream issue #4292: Go SDK: a manual consumer-group commit to a partition whose primary is not the coordinator drops the group membership and is refused

| | |
| --- | --- |
| Upstream issue | [apache/iggy#4292](https://github.com/apache/iggy/issues/4292) |
| Upstream failing test | [apache/iggy#4293](https://github.com/apache/iggy/pull/4293) (draft; branch `aperiodic-io:test/go-group-offset-store-membership`) |
| Status | Open upstream; no fix in this fork. The suggested fix is below and as a `TODO(#4292)` in the upstream PR. |
| Reported | 2026-09-24 |

## Why this matters for our fork

- **Found by:** building the Bento `iggy` input (aperiodic-io/bento#2) against a 3-node cluster.
- **Verified on:** `apache/iggy:0.9.0` + Go SDK `v0.9.0` (what this fork is based on) and `apache/iggy:edge`; failing
  test on upstream `master` @ `5d8129e95`.
- **Impact on us:** any Go consumer that commits offsets manually in a consumer group (the Bento input's
  at-least-once mode) loses its membership whenever a partition's primary is not the node it is connected to. The
  Bento PR re-syncs or rejoins when a poll is fenced, but a commit refused this way still fails until the SDK routes
  offset writes to the partition primary.

## Report as filed upstream

### Bug description

In a cluster, `StoreConsumerOffset` from the Go SDK for a consumer group fails
with `ConsumerGroupPartitionNotOwned` when the partition's primary is not the
node the client is connected to (the metadata leader). The member does own the
partition. After the failure the client is no longer a member of the group.

What happens:

1. The client is connected to the metadata leader (the coordinator) and has
   joined group `g`, which owns partition 0.
2. `StoreConsumerOffset(group g, partition 0)` goes out on the coordinator
   session. The coordinator is not partition 0's primary, so it answers
   `TransientNotAccepted`.
3. `sendFrame` walks the roster (`settleOnNextEndpoint`) and reconnects to the
   partition primary. The reconnect registers a **new client identity**.
4. The replayed commit comes from that new identity, which is not a member.
   The server refuses it: `consumer group member with client id: 0 does not own
   partition: 0 at the current generation (rebalance in progress)`. The server
   logs the real client id, e.g. `Consumer group member with client ID:
   3440707279 does not own partition: 0 ... operation=StoreConsumerOffset`.
5. The session has moved off the coordinator, and the group membership belongs
   to the old identity. From then on, group polls return the re-sync sentinel,
   `SyncConsumerGroup` returns `ConsumerGroupMemberNotFound`, and every later
   commit is refused until the caller calls `JoinConsumerGroup` again.

**Expected:** the commit reaches the partition primary, and the member keeps its
identity, its session on the coordinator and its membership. This is what the
Rust SDK does: `store_consumer_offset` goes through
`PollRouter::write_offset` over the consumer-session data connection, and
`assert_group_offset_routing` in `partition_primary_routing.rs` checks exactly
this. It is also what the Go SDK already does for **auto-commit polls**
(`pollPrimary` plus consumer-session attachment, covered by
`TestE2E_SplitPrimaryPollsPreserveCoordinatorMembership`).

**Actual:** manual commits (and `DeleteConsumerOffset`, which uses the same
path) go through the generic `c.do` failover. They lose the membership and are
refused.

A consumer that commits only after its output acknowledges (at-least-once, no
auto-commit) cannot store progress for such partitions at all.

### Affected area / component

Go SDK, Clustering / replication

### Deployment

Compiled from source (the integration harness). Also reproduced against the
DockerHub images.

### Versions

- `master` @ `5d8129e95`: failing test below (the Go SDK in `foreign/go` at
  that commit)
- DockerHub `apache/iggy:0.9.0` (`sha256:b35fc284…`) with Go SDK `v0.9.0`, and
  `apache/iggy:edge` (`sha256:8cc2475a…`, revision `85a397d0`) with Go SDK
  `v0.9.1-0.20260924080616-85a397d0e5fd`. `foreign/go` is unchanged between
  `v0.9.0` and master.

### Hardware / environment

Integration harness (3 nodes) in a Linux arm64 container (`rust:1.98`,
Go 1.25). Also seen on a 3-node Docker cluster on macOS: after a leader failover
in a long-running consumer, and on some freshly started clusters, whenever a
partition's primary differed from the metadata leader.

### Sample code

```go
require.NoError(t, client.JoinConsumerGroup(ctx, stream, topic, groupID))
consumer := iggcon.NewGroupConsumer(groupID)
partition := uint32(0)
// Partition 0's primary is not the node the client is connected to.
err := client.StoreConsumerOffset(ctx, consumer, stream, topic, 0, &partition)
// err: consumer group member with client id: 0 does not own partition: 0 at the
// current generation (rebalance in progress).
// The connection moved from the coordinator to the partition primary.
```

### Logs

```text
--- FAIL: TestE2E_SplitPrimaryManualCommitPreservesMembership (4.11s)
    Error: Received unexpected error:
           consumer group member with client id: 0 does not own partition: 0 at the current generation (rebalance in progress).
    Messages: manual group commit of offset 0 must reach the partition primary as a member (session 127.0.0.1:20016 -> 127.0.0.1:20000)

server: WARN shard-3 server::dispatch::partition: partition request with unresolved namespace; replying denied
        error=Consumer group member with client ID: 3440707279 does not own partition: 0 at the current generation (rebalance in progress). operation=StoreConsumerOffset
```

(`client id: 0` / `partition: 0` in the SDK's error string are placeholders. The
Go error type does not carry the server's values.)

### Reproduction

The failing test is on branch `test/go-group-offset-store-membership` (PR linked
below). It reuses the existing split-primary fixture (`seed_split_primaries`),
which moves metadata leadership off partition 0's primary:

```bash
cargo build --bin iggy-server --bin iggy
cargo test -p integration --test mod \
  given_split_primaries_when_go_group_commits_manually_should_preserve_membership \
  -- --ignored
```

It failed 3 of 3 runs. The existing auto-commit sibling
(`given_split_primaries_when_go_group_auto_commits_should_preserve_membership`)
passed in the same runs.

### Contribution

- [ ] I'm willing to submit a pull request to fix this bug
      (the failing test is in the linked PR; the fix is left to the SDK maintainers)
