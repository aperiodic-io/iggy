<!-- markdownlint-disable MD013 -->

# Upstream issue #4284: Partition replica commits its own stale prepare after rejoining a newer view (committed logs diverge)

| | |
| --- | --- |
| Upstream issue | [apache/iggy#4284](https://github.com/apache/iggy/issues/4284) |
| Upstream failing test | [apache/iggy#4285](https://github.com/apache/iggy/pull/4285) (draft; branch `aperiodic-io:test/stale-prepare-commit-after-view-change`) |
| Status | Open upstream; no fix in this fork. The suggested fix is below and as a `TODO(#4284)` in the upstream PR. |
| Reported | 2026-09-24 |

## Why this matters for our fork

- **Found by:** our message-dedup fuzz campaign on this fork (base `server-0.9.0`), as replicas holding different
  payloads or batch timestamps at the same committed offset, with dedup disabled too. The root cause was traced and
  the focused test written on upstream `master` @ `5d8129e95`; the same code paths exist at `server-0.9.0` (the
  focused test itself was not run there).
- **Impact on `iggy-raw-internal`:** a replica partitioned while holding an uncommitted tail can later serve a message
  nobody acked, at an offset where the other replicas serve the acked one. Consumers reading from different replicas
  would see different data at the same offset. Our dedup index is built only from committed content, so it would
  inherit the divergence rather than cause it.
- **Our dedup code is not involved:** the upstream reproduction runs on plain `master` with no fork code.

## Report as filed upstream

### Bug description

A partition replica that journaled a prepare in view `v` and never got it
replicated can later **commit that stale prepare** after it adopts a newer view
whose log committed a *different* prepare at the same op. After that, consumers
reading from that replica get a different message at the same offset than
consumers reading from the other replicas.

No crash or restart is needed. A network partition around the primary is
enough:

1. Replica 0 is primary of view 0. Ops 1..3 commit everywhere.
2. Replica 0 is cut off from its peers. A client sends it a request (`"stale"`).
   Replica 0 journals it as op 4 (offset 3). No peer ever receives it.
3. Replicas 1 and 2 elect view 1 and commit different requests
   (`"fresh-0"`..`"fresh-3"`) at ops 4..7 (offsets 3..6).
4. The partition heals. Replica 0 adopts view 1 and catches up.

**Expected:** replica 0's committed log equals the view's committed log. Its
op 4 is `"fresh-0"`, and `"stale"` was never committed anywhere.

**Actual:** replica 0 commits and serves `"stale"` at offset 3 (and
`"fresh-1".."fresh-3"` at offsets 4..6). The `"stale"` request was
never acknowledged to its producer, yet it shows up as committed on one
replica. Every other replica serves `"fresh-0"` at that offset, the message that
was acknowledged.

### Affected area / component

Clustering / replication (partition plane VSR)

### Deployment

Compiled from source (reproduced in the deterministic simulator)

### Versions

`master` @ `5d8129e95` (chore(deps): Bump the github-actions group ... #4268)

### Hardware / environment

Deterministic simulator (`core/simulator`), Linux container, pinned toolchain
from `rust-toolchain.toml`. No real I/O involved.

### Reproduction

A focused failing simulator test is on branch
`test/stale-prepare-commit-after-view-change` (see the upstream PR above):

```bash
cargo test -p simulator --lib stale_prepare_commit_tests
```

The test is `core/simulator/src/stale_prepare_commit_tests.rs`. It uses the
default (fault-free) network and one explicit fault: replica 0's replica links
are blocked in both directions (`Network::set_link_filter`) and then restored.
Before its final check it asserts that the scenario really happened: the warmup
committed, the majority elected view 1, the new view committed through op >= 5,
and replica 0 caught up to that commit point. The final check compares the logs
served by the ordinary client poll path (`Simulator::poll_messages`) on replica 0
and on the new primary.

### Logs

```text
---- stale_prepare_commit_tests::given_an_old_primary_holding_an_uncommitted_prepare_when_it_rejoins_a_view_that_committed_another_prepare_at_that_op_should_not_commit_its_own stdout ----
panicked at core/simulator/src/stale_prepare_commit_tests.rs:223:5:
assertion `left == right` failed: the rejoined replica serves a committed log that differs from the view's
  left: [(0, "warmup-0"), (1, "warmup-1"), (2, "warmup-2"), (3, "stale"), (4, "fresh-1"), (5, "fresh-2"), (6, "fresh-3")]
 right: [(0, "warmup-0"), (1, "warmup-1"), (2, "warmup-2"), (3, "fresh-0"), (4, "fresh-1"), (5, "fresh-2"), (6, "fresh-3")]
```

A temporary trace of the same run (local instrumentation, not in the PR) shows
what happened on replica 0 (checksums truncated to 32 bits):

```text
r0 RECONCILE view=1 applied_floor=3 announced_commit=0 op_head=None journal_last_op=Some(4) canonical=[]
r0 COMMIT view=1 op=4 v=0 ck=d7897258 par=9112f630 ... base_offset=3 [3:stale]      <- its own view-0 prepare
r0 APPEND(repair) op=5 v=1 ck=59679ca0 par=9becabbd ... [4:fresh-1]                  <- parent is the canonical op 4, not d7897258
r1 COMMIT view=1 op=4 v=1 ck=9becabbd par=9112f630 ... base_offset=3 [3:fresh-0]     <- what the view committed
```

### Root cause (file:line on `5d8129e95`)

1. **The StartView does not vouch for ops at or below its commit point.**
   `VsrConsensus::handle_start_view` (`core/consensus/src/impls.rs:3329`) sets
   `log_view` (`:3428`), adopts the suffix (`:3440`) and emits `CommitJournal`.
   The merged log that seeds the suffix only carries canonical headers for
   `commit_max..=op_head` (`core/consensus/src/dvc_merge.rs:83`). In this run
   the suffix is empty (`pending == None`).
2. **Reconciliation only looks at what the suffix names.**
   `reconcile_partition_view_divergence` (`core/shard/src/lib.rs:11339`, called
   from `:4723` on StartView adoption and `:5869`) compares local headers only
   against `pending.headers` (`:11353`), and otherwise truncates only above the
   announced head (`:11386`). A leftover op in `(commit_min, commit_max]` from the
   old view is neither compared nor truncated.
3. **The commit walk applies whatever header the journal holds.** A backup's
   `commit_journal` falls back to `collect_committable_from_journal`
   (`core/partitions/src/iggy_partition.rs:5266`, `:5346`). That calls
   `PartitionJournal::committed_headers_from` (`core/partitions/src/journal.rs:938`),
   which takes the first resident header for each op (`:988`). It checks neither
   identity against the view's log nor the `parent` chain. Here the repaired op 5
   has `parent = 9becabbd` while the local op 4 is `d7897258`, so the break is
   visible but nothing looks at it.
4. **Repair cannot replace the stale entry.** `apply_repaired_prepare` returns
   early when `holds_op(header.op)` is true
   (`core/partitions/src/iggy_partition.rs:8140`). It checks presence, not
   identity, so the canonical op 4 is dropped even if it is fetched.

Relation to the `checksum_body = 0` TODO at `core/consensus/src/impls.rs:4089`:
**it is not the cause here.** The two prepares at op 4 already have different
header checksums (`d7897258` vs `9becabbd`), because the header checksum covers
`client`, `request`, `request_checksum`, `timestamp`, `commit` and `parent`. Any
header-identity comparison would catch them. The bug is that no comparison runs
for ops the StartView suffix does not name.

### Impact

- Committed-log divergence across replicas for the same partition and offset.
  A consumer's result depends on which replica serves the poll.
- An unacknowledged (timed-out) produce shows up as committed on one replica.
  The acknowledged message at that offset is missing there.
- If the divergent replica is later elected primary, or serves repair, the
  divergence can spread. (Plausible from the code, not tested.)
- The same mechanism shows up under crash/restart when the journal survives the
  restart, for example a persisted topic whose WAL replay restores an
  uncommitted tail from an older view. We found it that way first, with a
  randomized restart harness. When the harness restart kept only committed data, every
  divergent-payload and divergent-timestamp finding went away (seeds 1..60: 11 seeds
  had one before and 0 after; also 0 after with the frontier restore on). The partition-only reproduction above is
  the minimal form.

### Suggested fix

In `reconcile_partition_view_divergence`, when the adopted view is newer than the
`log_view` this replica last held, treat every journaled op above `commit_min`
(the applied floor) as unvouched: truncate from `applied_floor + 1` and let
journal repair refetch the canonical prepares. That is the same rule the
function already applies above the head. Alternatively, keep the entries but
make the commit walk verify the `parent` hash chain from a canonical anchor
(for example the op the StartView names, walking down) before it may apply an
entry. Either way, `apply_repaired_prepare` should replace an entry whose
checksum differs from the canonical one instead of skipping on `holds_op`.

### Contribution

- [ ] I'm willing to submit a pull request to fix this bug
      (the failing test is ready as a PR; the fix is open for discussion)
