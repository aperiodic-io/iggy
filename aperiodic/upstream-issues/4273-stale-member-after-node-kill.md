<!-- markdownlint-disable MD013 -->

# Upstream issue #4273: consumer group keeps a dead member after its node is killed

| | |
| --- | --- |
| Upstream issue | [apache/iggy#4273](https://github.com/apache/iggy/issues/4273) (reported by someone else; we added [this trigger](https://github.com/apache/iggy/issues/4273#issuecomment-5818309071)) |
| Upstream fix | In progress by a maintainer on `fix/4273-stale-consumer-group-members` (heartbeat-based expiry); not yet tested against our trigger |
| Repro | [gist](https://gist.github.com/almostintuitive/cbff837fce0709ccaa209d1842e957f0), also inlined below |
| Status | Open upstream; no fix in this fork |
| Reported | 2026-09-24 |

## Why this matters for our fork

- **Found by:** building the Bento `iggy` input (aperiodic-io/bento#2) against a 3-node cluster.
- **Verified on:** `apache/iggy:0.9.0` (this fork's base) and `apache/iggy:edge`, 2/2 runs each.
- **Impact on `iggy-raw-internal`:** when the node that holds a consumer's connection dies (a node failure or a
  rollout), that consumer's old group member keeps its partitions forever. Those partitions stop being consumed, and
  neither a new consumer nor restarting the dead node recovers them. Until upstream fixes it, raw archivers should
  avoid consumer groups (use one consumer per partition), or delete and recreate the group after a node loss.

## Our report (comment on the upstream issue)

Another trigger for the same stale member, this time in a 3-node cluster and fully deterministic: **kill the replica that holds the member's connection**.

When the node that owns a client's TCP connection dies, nothing submits that client's disconnect `Logout`. The surviving replicas still hold the session in the replicated client table, so the member keeps its partitions. The SDK reconnects to a survivor under a new client id, rejoins, and gets only what is left. A brand-new consumer is in the same position. Restarting the killed node does not remove the member either.

Reproduces 2/2 runs of [this repro](https://gist.github.com/almostintuitive/cbff837fce0709ccaa209d1842e957f0) on `apache/iggy:0.9.0` (`sha256:b35fc284…`, Go SDK `v0.9.0`) and 2/2 on `apache/iggy:edge` (`sha256:8cc2475a…`, image revision `85a397d0`, Go SDK `v0.9.1-0.20260924080616-85a397d0e5fd`). Both images report `server 0.9.0`.

### Repro

The [repro](https://gist.github.com/almostintuitive/cbff837fce0709ccaa209d1842e957f0) (`cluster.sh`, `run.sh`, `main.go`) depends only on the Docker image and the Go SDK. `cluster.sh` starts a 3-node VSR cluster on a bridge network at `172.29.0.10-12`, and `run.sh` wraps it:

```bash
./run.sh stale-member apache/iggy:edge 85a397d0e5fd36262a3bbc203165c7a5e01b9029
```

Steps (`main.go`, `staleMember`):

1. Create a topic with 3 partitions and group `g`.
2. Connect a consumer to the metadata leader, join `g`, and sync. It gets `[0 1 2]`.
3. `docker kill` the leader. The SDK reconnects to a survivor, sync returns `member not found`, and the consumer rejoins.
4. Watch the group for 2 minutes, join a brand-new consumer, then `docker start` the killed node and wait 40 s.

Output on edge:

```text
[   1.1s] metadata leader: iggy-0 (172.29.0.10:8090)
[   1.2s] consumer joined: sync generation=1 partitions=[0 1 2]
[   1.2s] group: members=1 [{ID:0 PartitionsCount:3 Partitions:[0 1 2]}]
[   1.2s] docker kill iggy-0
[   6.4s] consumer sync after kill: error: consumer group member with id: 0 for group with id: 0 for topic with id: 0 was not found. (now via 172.29.0.11:8090)
[   6.4s] consumer rejoined
[   6.5s] consumer sync after rejoin: generation=2 partitions=[2]
[   8.1s] group: members=2 [{ID:0 PartitionsCount:2 Partitions:[0 1]} {ID:1 PartitionsCount:1 Partitions:[2]}] | consumer sync: generation=2 partitions=[2]
   ... unchanged every 15 s until ...
[ 128.1s] group: members=2 [{ID:0 PartitionsCount:2 Partitions:[0 1]} {ID:1 PartitionsCount:1 Partitions:[2]}] | consumer sync: generation=2 partitions=[2]
[ 128.6s] a brand-new consumer joined: sync generation=3 partitions=[1]
[ 128.6s] docker start iggy-0
[ 169.1s] after the killed node restarted (40s): group: members=2 [{ID:0 PartitionsCount:2 Partitions:[0 2]} {ID:1 PartitionsCount:1 Partitions:[1]}] | consumer sync: generation=4 partitions=[1]
```

Member `0` belongs to the dead connection. It outlives `consumer_group.rebalancing_timeout` (30 s) and the restart of its node. Each rebalance still hands it partitions: first `[0 1]`, then `[0 2]`.

A clean disconnect on a live node does not leak: with the same cluster and SDK, a client whose connection is closed while its node stays up is removed immediately (the group drops to `members=0`).

### Where it seems to come from (hypothesis, read at `85a397d0`)

This matches your analysis in the report, with one more path that never submits the `Logout`:

- The only runtime removal is the `Logout` apply in `core/metadata/src/impls/metadata.rs:645-659`. It calls `remove_consumer_group_member`, which is `core/metadata/src/stm/stream.rs:1561`.
- That `Logout` is submitted by the replica that owned the transport (`submit_disconnect_logout`, `core/server/src/dispatch/session_ops.rs:1039`, called from `:476` and `core/server/src/dispatch/mod.rs:188`). If that replica is killed, no survivor ever submits it.
- `Register` is replicated, so the survivors keep the dead client's session in their client table. A cleanup keyed on "client id not in the client table" would not catch this case. A liveness or heartbeat-based expiry would.

I saw `fix/4273-stale-consumer-group-members` (`23492f133`), with heartbeat reports on the metadata primary and expiry via `Logout`. I have not run this repro against it. If there's an image from that branch, `./run.sh stale-member <image>` checks it: exit code 1 means the member is still there after the restart, 0 means it was removed. The replica that owned the dead connection is exactly the one that stops reporting, so this case exercises the "a crashed node must stop deferring expiry" path.

## Repro files

<details>
<summary>cluster.sh</summary>

```bash
#!/usr/bin/env bash
# Starts a 3-node Apache Iggy VSR cluster (IMAGE, default apache/iggy:0.9.0) on a docker
# bridge network with fixed addresses (the host must be able to reach
# 172.29.0.0/24). `cluster.sh down` removes it again.
set -euo pipefail
IMAGE=${IMAGE:-apache/iggy:0.9.0}
NET=iggy-cluster-test
for i in 0 1 2; do docker rm -fv iggy-$i >/dev/null 2>&1 || true; done
docker network rm "$NET" >/dev/null 2>&1 || true
[ "${1:-up}" = down ] && exit 0
docker network create --subnet 172.29.0.0/24 "$NET" >/dev/null
roster=()
for i in 0 1 2; do
  roster+=(-e IGGY_CLUSTER_NODES_${i}_NAME=iggy-$i -e IGGY_CLUSTER_NODES_${i}_IP=172.29.0.1$i
           -e IGGY_CLUSTER_NODES_${i}_REPLICA_ID=$i -e IGGY_CLUSTER_NODES_${i}_PORTS_TCP=8090
           -e IGGY_CLUSTER_NODES_${i}_PORTS_QUIC=8080 -e IGGY_CLUSTER_NODES_${i}_PORTS_HTTP=3000
           -e IGGY_CLUSTER_NODES_${i}_PORTS_WEBSOCKET=8092 -e IGGY_CLUSTER_NODES_${i}_PORTS_TCP_REPLICA=9090)
done
for i in 0 1 2; do
  # Iggy needs io_uring, which the default seccomp profile blocks.
  docker run -d --name iggy-$i --hostname iggy-$i --network "$NET" --ip 172.29.0.1$i \
    --memory 2g --security-opt seccomp=unconfined \
    -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
    -e IGGY_TCP_ADDRESS=172.29.0.1$i:8090 -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
    -e IGGY_QUIC_ENABLED=false -e IGGY_WEBSOCKET_ENABLED=false \
    -e IGGY_SHARDING_CPU_ALLOCATION=1 -e IGGY_MEMORY_POOL_SIZE=512MiB \
    -e IGGY_CLUSTER_ENABLED=true -e IGGY_CLUSTER_AUTH_ENABLED=true \
    -e IGGY_CLUSTER_AUTH_SHARED_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef \
    -e IGGY_CLUSTER_NAME=repro \
    "${roster[@]}" "$IMAGE" --replica-id $i >/dev/null
done
```

</details>

<details>
<summary>run.sh</summary>

```bash
#!/usr/bin/env bash
# Usage: ./run.sh <stale-member|cancel-drops-membership> [image] [go-sdk-version]
#   ./run.sh stale-member apache/iggy:0.9.0 v0.9.0
#   ./run.sh stale-member apache/iggy:edge  85a397d0e5fd36262a3bbc203165c7a5e01b9029
# Starts a fresh 3-node cluster, runs the repro, removes the cluster.
# Exit code 1 means reproduced, 0 means not reproduced, 2 means setup failure.
set -euo pipefail
cd "$(dirname "$0")"
REPRO=$1 IMAGE=${2:-apache/iggy:0.9.0} SDK=${3:-v0.9.0}
go get "github.com/apache/iggy/foreign/go@$SDK" >/dev/null 2>&1
go mod tidy >/dev/null 2>&1
echo "image: $IMAGE ($(docker image inspect "$IMAGE" --format '{{index .RepoDigests 0}}'), $(docker run --rm "$IMAGE" --version 2>&1 | tail -1))"
echo "go sdk: $(go list -m github.com/apache/iggy/foreign/go)"
IMAGE=$IMAGE ./cluster.sh
trap './cluster.sh down' EXIT
set +e
go run . "$REPRO"
code=$?
set -e
exit $code
```

</details>

<details>
<summary>main.go</summary>

```go
// Reproductions for two Apache Iggy consumer-group issues on a 3-node VSR
// cluster started by cluster.sh (nodes iggy-0..2 at 172.29.0.10..12:8090).
//
//    go run . stale-member   # (A) a member whose node died never leaves the group
//    go run . cancel-drops-membership  # (B) a cancelled request silently drops group membership
//
// Only the official Go SDK and the docker CLI are used.
package main

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "os/exec"
    "strings"
    "time"

    "github.com/apache/iggy/foreign/go/client"
    "github.com/apache/iggy/foreign/go/client/tcp"
    iggcon "github.com/apache/iggy/foreign/go/contracts"
)

var (
    addrs   = []string{"172.29.0.10:8090", "172.29.0.11:8090", "172.29.0.12:8090"}
    nodes   = []string{"iggy-0", "iggy-1", "iggy-2"}
    start   = time.Now()
    ctx     = context.Background()
    verbose = os.Getenv("SDK_DEBUG") != ""
)

func logf(format string, args ...any) {
    fmt.Printf("[%6.1fs] %s\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
}

func must[T any](v T, err error) T {
    if err != nil {
        logf("FATAL: %v", err)
        os.Exit(2)
    }
    return v
}

func id(s string) iggcon.Identifier { return must(iggcon.NewIdentifier(s)) }

// connect signs in through the given seed, retrying while the cluster boots.
func connect(addr string) iggcon.Client {
    level := slog.LevelError + 1
    if verbose {
        level = slog.LevelDebug
    }
    for i := 0; ; i++ {
        c := must(client.NewIggyClient(
            client.WithTcp(tcp.WithServerAddress(addr),
                tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials("iggy", "iggy"))),
            client.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))),
        ))
        cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
        err := c.Connect(cctx)
        cancel()
        if err == nil {
            return c
        }
        _ = c.Close()
        if i == 30 {
            logf("FATAL: connect %s: %v", addr, err)
            os.Exit(2)
        }
        time.Sleep(2 * time.Second)
    }
}

func leader(c iggcon.Client) int {
    md := must(c.GetClusterMetadata(ctx))
    for _, n := range md.Nodes {
        if n.Role == iggcon.RoleLeader {
            for i, a := range addrs {
                if strings.HasPrefix(a, n.IP+":") {
                    return i
                }
            }
        }
    }
    logf("FATAL: no leader in %+v", md.Nodes)
    os.Exit(2)
    return -1
}

func docker(args ...string) {
    if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
        logf("FATAL: docker %v: %v %s", args, err, out)
        os.Exit(2)
    }
}

func setupTopic(c iggcon.Client, partitions uint32) (iggcon.Identifier, iggcon.Identifier, iggcon.Identifier, string) {
    if _, err := c.GetStream(ctx, id("repro")); err != nil {
        must(c.CreateStream(ctx, "repro"))
    }
    name := fmt.Sprintf("t%d", time.Now().UnixNano())
    must(c.CreateTopic(ctx, id("repro"), name, partitions, iggcon.CompressionAlgorithmNone,
        iggcon.IggyExpiryNeverExpire, 64<<20, iggcon.SegmentSizeOption(8<<20)))
    must(c.CreateConsumerGroup(ctx, id("repro"), id(name), "g"))
    // One message per partition, so offset 0 exists everywhere.
    for p := range partitions {
        m := must(iggcon.NewIggyMessage([]byte("x")))
        must(c.SendMessages(ctx, id("repro"), id(name), iggcon.PartitionId(p), []iggcon.IggyMessage{m}))
    }
    return id("repro"), id(name), id("g"), name
}

func members(admin iggcon.Client, s, t, g iggcon.Identifier) string {
    d, err := admin.GetConsumerGroup(ctx, s, t, g)
    if err != nil {
        return "error: " + err.Error()
    }
    return fmt.Sprintf("members=%d %+v", d.MembersCount, d.Members)
}

func sync(c iggcon.Client, s, t, g iggcon.Identifier) string {
    a, err := c.SyncConsumerGroup(ctx, s, t, g)
    if err != nil {
        return "error: " + err.Error()
    }
    return fmt.Sprintf("generation=%d partitions=%v", a.Generation, a.Partitions)
}

//------------------------------------------------------------------------------

// (A) A consumer joins a group through the metadata leader, the leader is
// killed, and the consumer (reconnected by the SDK under a new identity)
// rejoins. The dead identity stays a member with its partitions.
func staleMember() {
    admin := connect(addrs[0])
    l := leader(admin)
    other := (l + 1) % 3
    logf("metadata leader: %s (%s)", nodes[l], addrs[l])
    admin = connect(addrs[other]) // an admin session that survives the kill
    s, t, g, name := setupTopic(admin, 3)
    logf("created topic repro/%s with 3 partitions and consumer group g", name)

    consumer := connect(addrs[l])
    logf("consumer connected via %s", consumer.GetConnectionInfo().ServerAddress)
    must(0, consumer.JoinConsumerGroup(ctx, s, t, g))
    logf("consumer joined: sync %s", sync(consumer, s, t, g))
    logf("group: %s", members(admin, s, t, g))

    logf("docker kill %s", nodes[l])
    docker("kill", nodes[l])
    time.Sleep(3 * time.Second)

    logf("consumer sync after kill: %s (now via %s)", sync(consumer, s, t, g), consumer.GetConnectionInfo().ServerAddress)
    if err := consumer.JoinConsumerGroup(ctx, s, t, g); err != nil {
        logf("consumer rejoin: %v", err)
    } else {
        logf("consumer rejoined")
    }
    logf("consumer sync after rejoin: %s", sync(consumer, s, t, g))
    for i := 0; i <= 8; i++ {
        logf("group: %s | consumer sync: %s", members(admin, s, t, g), sync(consumer, s, t, g))
        if i < 8 {
            time.Sleep(15 * time.Second)
        }
    }

    fresh := connect(addrs[other])
    must(0, fresh.JoinConsumerGroup(ctx, s, t, g))
    logf("a brand-new consumer joined: sync %s", sync(fresh, s, t, g))
    must(0, fresh.LeaveConsumerGroup(ctx, s, t, g))
    must(0, fresh.Close())

    logf("docker start %s", nodes[l])
    docker("start", nodes[l])
    time.Sleep(40 * time.Second)
    logf("after the killed node restarted (40s): group: %s | consumer sync: %s", members(admin, s, t, g), sync(consumer, s, t, g))

    d := must(admin.GetConsumerGroup(ctx, s, t, g))
    if d.MembersCount > 1 {
        logf("REPRODUCED: the killed node's member is still in group g, holding partitions the live consumer never gets")
        os.Exit(1)
    }
    logf("NOT REPRODUCED: the dead member was removed")
}

//------------------------------------------------------------------------------

// (B) A group member's in-flight request is cancelled (a context deadline, or
// shutdown). The SDK tears the connection down to abandon the reply; the next
// request reconnects under a new client identity, which is not a member. The
// SDK does not rejoin the group it joined explicitly, so stores for owned
// partitions are refused and explicit-partition polls return the re-sync
// sentinel, while nothing surfaced the membership loss to the caller.
func cancelDropsMembership() {
    admin := connect(addrs[0])
    s, t, g, name := setupTopic(admin, 3)
    payload := make([]byte, 1024)
    var msgs []iggcon.IggyMessage
    for range 1000 {
        msgs = append(msgs, must(iggcon.NewIggyMessage(payload)))
    }
    for range 10 {
        must(admin.SendMessages(ctx, s, t, iggcon.PartitionId(0), msgs))
    }
    logf("created topic repro/%s (3 partitions, 10001 x 1 KiB messages in partition 0) and consumer group g", name)

    c := connect(addrs[0])
    must(0, c.JoinConsumerGroup(ctx, s, t, g))
    cons := iggcon.NewGroupConsumer(g)
    p0 := uint32(0)
    logf("member joined via %s: sync %s", c.GetConnectionInfo().ServerAddress, sync(c, s, t, g))
    logf("group: %s", members(admin, s, t, g))
    logf("store offset 0 of partition 0 before: err=%v", c.StoreConsumerOffset(ctx, cons, s, t, 0, &p0))

    // Cancel a large poll while it is in flight.
    var err error
    for i := 0; i < 50; i++ {
        pctx, cancel := context.WithTimeout(ctx, 500*time.Microsecond)
        _, err = c.PollMessages(pctx, s, t, cons, iggcon.OffsetPollingStrategy(0), 10000, false, &p0)
        cancel()
        if err != nil {
            break
        }
    }
    logf("poll with a context cancelled mid-flight: err=%v", err)
    if err == nil {
        logf("SETUP: could not cancel a poll in flight")
        os.Exit(2)
    }

    reproduced := false
    for p := uint32(0); p < 3; p++ {
        serr := c.StoreConsumerOffset(ctx, cons, s, t, 0, &p)
        logf("StoreConsumerOffset(partition %d, offset 0): err=%v (connection %s)", p, serr, c.GetConnectionInfo().ServerAddress)
        if serr != nil {
            reproduced = true
        }
    }
    pm, perr := c.PollMessages(ctx, s, t, cons, iggcon.OffsetPollingStrategy(0), 1, false, &p0)
    if perr != nil {
        logf("group poll of partition 0: err=%v", perr)
    } else {
        logf("group poll of partition 0: err=nil partition_id=%#x messages=%d", pm.PartitionId, len(pm.Messages))
    }
    logf("sync: %s", sync(c, s, t, g))
    logf("group: %s", members(admin, s, t, g))
    if reproduced {
        must(0, c.JoinConsumerGroup(ctx, s, t, g))
        logf("after an explicit JoinConsumerGroup: store err=%v, sync %s", c.StoreConsumerOffset(ctx, cons, s, t, 0, &p0), sync(c, s, t, g))
        logf("REPRODUCED: the explicitly joined membership was dropped by the SDK's reconnect and not restored")
        os.Exit(1)
    }
    logf("NOT REPRODUCED: stores still accepted after the cancelled request")
}

func main() {
    if len(os.Args) < 2 {
        fmt.Println("usage: go run . stale-member|cancel-drops-membership")
        os.Exit(2)
    }
    switch os.Args[1] {
    case "stale-member":
        staleMember()
    case "cancel-drops-membership":
        cancelDropsMembership()
    default:
        fmt.Println("unknown repro", os.Args[1])
        os.Exit(2)
    }
}
```

</details>

<details>
<summary>go.mod</summary>

```text
module iggyrepro

go 1.25.0

require github.com/apache/iggy/foreign/go v0.9.1-0.20260924080616-85a397d0e5fd

require (
    github.com/avast/retry-go/v5 v5.0.0 // indirect
    github.com/google/uuid v1.6.0 // indirect
    github.com/klauspost/compress v1.20.0 // indirect
    github.com/klauspost/cpuid/v2 v2.2.10 // indirect
    github.com/zeebo/xxh3 v1.1.0 // indirect
    golang.org/x/sys v0.30.0 // indirect
)
```

</details>
