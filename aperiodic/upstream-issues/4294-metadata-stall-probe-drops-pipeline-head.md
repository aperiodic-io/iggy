<!-- markdownlint-disable MD013 -->

# Upstream issue #4294: Metadata primary panics "committed prepare ... must be in journal" after its walk-stall detector truncates the pipeline head

| | |
| --- | --- |
| Upstream issue | [apache/iggy#4294](https://github.com/apache/iggy/issues/4294) |
| Upstream failing test | [apache/iggy#4295](https://github.com/apache/iggy/pull/4295) (draft; branch `aperiodic-io:test/metadata-walk-drops-pipeline-head`) |
| Status | Open upstream; no fix in this fork. The suggested fix is below and as a `TODO(#4294)` in the upstream PR. |
| Reported | 2026-09-24 |

## Why this matters for our fork

- **Found by:** our message-dedup fuzz campaign on this fork (one seed hit `committed prepare ... must be in journal`).
  Our dedup code lives in the partition plane and does not touch the metadata plane.
- **Verified on:** upstream `master` @ `5d8129e95` **and** `server-0.9.0` (`71f29618b`, this fork's base), same seed,
  same panic.
- **Impact on `iggy-raw-internal`:** rare (about 1 in 400–1500 fuzz seeds), but it is a crash of the freshly elected
  metadata primary right after a view change, i.e. exactly during a node failure.

## Report as filed upstream

### Bug description

A metadata replica that becomes primary through a view change can panic with:

```text
on_ack: committed prepare op=3 checksum=... must be in journal
```

The prepare wasn't lost by storage or by the network. The shard's own
walk-stall detector deleted it from the WAL one tick earlier, while the primary's
pipeline still held it as the next op to commit.

Sequence (simulator, 3 replicas, only the three client `Register` ops on the
metadata plane):

1. Replica 2 learns `commit_max = 3` but has applied only through op 2
   (`commit_min = 2`).
2. Replicas 0 and 1 crash and restart in turn, and replica 2 wins view 2. The DVC
   merge gives `op_head = 3, commit_max = 2`. Replica 2 holds the header and body
   for op 3, so `advance_pending_metadata_view` starts the view and rebuilds the
   pipeline over `3..=3`. Op 3 is now the pipeline head, and `commit_max` (3) is
   still above `commit_min` (2).
3. On the same tick, `tick_metadata`'s gap probe sees `normal`,
   `commit_min < commit_max`, and a resident header at `commit_min + 1 = 3`, so it
   calls the group walk-stalled.
4. `commit_journal` runs and deliberately stops at op 3, because the pipeline holds
   it and committing it is `on_ack`'s job. `commit_min` doesn't move.
5. The detector reads "the walk moved nothing" as "the header has no readable
   body". It calls `drop_unwalkable_metadata_entry`, which runs
   `truncate_from(3)`:
   `metadata commit walk found a resident header with no body at op 3; dropped 1 entries`.
   The body was readable. `commit_journal` never logged `prepare body missing`.
6. The next `commit_committable_prefix` peeks the pipeline head (op 3, committed)
   and can't find it in the journal, which triggers the panic.

**Expected:** the primary commits op 3 from its pipeline, and nothing in the WAL
is dropped.

**Actual:** the WAL entry the pipeline depends on is truncated, and the replica
panics. In production this is a process crash of the newly elected metadata
primary.

### Affected area / component

Metadata, Clustering / replication

### Deployment

Compiled from source (reproduced in the deterministic simulator)

### Versions

`master` @ `5d8129e95` (chore(deps): Bump the github-actions group ... #4268).
Also reproduces at `server-0.9.0` (`71f29618b`), with the same seed and the same
panic.

### Hardware / environment

Deterministic simulator (`core/simulator`), Linux container, pinned toolchain
from `rust-toolchain.toml`. No real I/O involved.

### Reproduction

A focused failing simulator test is on branch
`test/metadata-walk-drops-pipeline-head` (see the upstream PR above):

```bash
cargo test --release -p simulator --lib \
  metadata_walk_stuck_detector_tests
```

The test uses swarm network seed 1189 and three clients that only `Register`,
with no client or metadata workload traffic. It injects three replica faults:
crash r0 at step 297, restart r0 at step 705, and crash r1 at step 1151. It
panics about 160 steps later. After the drain, it asserts that every replica has
applied the three registrations.

A broader campaign hits the same bug. In one process per seed, a crash/restart
fuzz loop over seeds 20000..20399 (three producers, swarm network) panicked on
seed 20115 at op 3. With client traffic removed, seeds 1..1500 gave one hit
(seed 1189).

### Logs

```text
INFO consensus::impls: view-change quorum merged; repairing up to the merged log before starting the view replica=2 view=2 op_head=3 commit_max=2
INFO shard: repairing toward the merged log before starting the view shard=0 missing_op=2 peer=0 to_op=3
INFO iggy.sim: sim_event="PrimaryElected" plane="metadata" replica_id=2 view=2 log_view=2 commit=3 status="normal" role="primary"
INFO shard: merged log is locally serveable; starting the view shard=0 view=2 op_head=3 commit_max=2
WARN shard: metadata commit walk found a resident header with no body at op 3; dropped 1 entries from it so repair can refill the range shard=0 stuck_op=3 removed=1
INFO shard: metadata journal repair walked shard=0 through_op=3 commit_min=2 done=false

thread '...' panicked at core/metadata/src/impls/metadata.rs:2833:21:
on_ack: committed prepare op=3 checksum=9359122765513648906 must be in journal
   2: IggyMetadata<..>::commit_committable_prefix::{closure#0}
   3: IggyShard<..>::run_message_pump::{closure#0}
```

`commit_journal: prepare body missing for op=3` never appears. The body was
readable, and the walk stopped at the pipeline head.

### Root cause (file:line on `5d8129e95`)

- `core/metadata/src/impls/metadata.rs:3683-3688` (`commit_journal`): the walk
  `break`s when `pipeline_head_header().op == commit_min + 1`, without advancing.
  This is intentional, because the pipeline head is committed by `on_ack` /
  `commit_committable_prefix`.
- `core/shard/src/lib.rs:10131-10136` (`metadata_gap_probe`): `next_op_resident`
  requires only that the header at `commit_min + 1` is resident, and doesn't
  exclude the op the pipeline holds. `group_is_walk_stalled`
  (`core/shard/src/lib.rs:10907`) is therefore true for a primary whose pipeline
  head is exactly `commit_min + 1` and `<= commit_max`.
- `core/shard/src/lib.rs:10281-10304` (`tick_metadata`): after `commit_journal`,
  `walked == probe.commit_min` is treated as "resident header, no body", and
  `drop_unwalkable_metadata_entry` (`core/shard/src/lib.rs:10052`, truncation at
  `:10064`) removes that op and everything above it from the WAL.
- `core/metadata/src/impls/metadata.rs:2822-2837` (`commit_committable_prefix`):
  the pipeline head is still there and `<= commit_max`, so the next driver reads
  it from the journal and panics.

The two halves disagree. The walk says the pipeline head isn't its op to commit,
and the detector treats the walk's refusal as proof that the body is missing.

### Impact

Crash of a freshly elected metadata primary right after a view change. It needs
the primary-elect to inherit a committed-but-unapplied op as its pipeline head at
exactly `commit_min + 1`, which crash/restart plus a lossy network produces. It's
rare in fuzzing (about 1 in 400 to 1 in 1500 seeds), but it's a panic, not a
stall.

### Suggested fix

In `metadata_gap_probe`, don't count the op the pipeline holds as a stalled
walk:

```rust
let next_op_resident = normal
    && !transferring
    && commit_min < commit_max
    && self.metadata_walk_stuck_op.get() != next_op
    && !consensus.pipeline_head_header().is_some_and(|head| head.op == next_op)
    && journal.handle().header(next_op as usize).is_some();
```

The pipeline head is already re-driven by `resume_stranded_commits` on the same
tick. With this one-line change, the new test passes, seed 20115 runs clean in
both frontier-restore modes, and the no-traffic campaign over seeds 1..1500 runs
1500/1500 clean. An alternative is to have `commit_journal` report why it stopped
(pipeline head vs. missing body) and truncate only on a missing body.

### Contribution

- [ ] I'm willing to submit a pull request to fix this bug
