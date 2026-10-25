# Decisions

Rulings made while building swimring, in the order they came up. Format:
`Ruling: what — why — cost if wrong`.

## Protocol

- Ruling: shuffled round-robin probe-target selection (SWIM §4.3), not uniform random — gives time-bounded completeness (every live member probed by every other within n−1 periods) and is what deployed implementations use — the paper's e/(e−1) analysis assumes random choice, so the eval prints both the paper's formula and the round-robin expectation; they converge for large n.
- Ruling: a node's own probe period ends at its next tick, and a direct ack that arrives after the probe timeout but before the period ends still counts — matches the paper (the indirect phase runs to the end of the period) and memberlist — slightly lower false-positive rate than a strict "direct ack within timeout" rule; the loss model in `experiment.ProbeFailureProbability` assumes exactly this behaviour and matches measurements to within 2 standard errors.
- Ruling: suspect/dead updates about unknown members are ignored, and alive updates without an address are ignored — an update can only add a member if it says how to reach it; avoids resurrecting purged tombstones — a member that is suspected before a node learns about it is only learned once it refutes (or via push-pull).
- Ruling: Dead records received in a full-state sync are downgraded to Suspect (memberlist behaviour) — lets a member falsely declared dead by the far side of a partition refute instead of being trusted blindly — slower removal of members that really died while partitioned (they pass through a suspicion timeout).
- Ruling: periodic "reconnect" sync with a random Dead member (default 10 s) — without it, two halves of a healed partition never talk again because each half stopped probing the other — some extra traffic to genuinely dead addresses until the tombstone is purged (default 5 min).
- Ruling: a node announces its own Alive on Start, and re-announces it when it sees a suspicion/death claim about itself at an older incarnation — found by a failing test: with pure piggybacking a refutation's λ·log n transmissions could all miss one member, which then timed the live node out; the buddy system delivers the stale claim on a ping and the re-announcement rides on the ack — a little more gossip.
- Ruling: Lifeguard parameters follow the paper and memberlist defaults: LHM saturates at S = 8, success −1, failed probe +1 or +(missed nacks) when nacks were requested, refutation +1; dynamic suspicion with α = 4, β = 6, k = α − 2 expected confirmations (0 when fewer than k+2 live members); relays nack at 80% of the probe timeout — reproducing known-good settings keeps the comparison with plain SWIM meaningful — other settings may trade detection latency differently.
- Ruling: each Lifeguard extension is individually switchable (`swim.Lifeguard`) — needed for the ablation table — none.
- Ruling: the Local Health Multiplier scales both the probe interval and the probe timeout — Lifeguard paper §IV — a degraded node probes (and therefore gossips) less, which the ablation shows makes it slower to refute suspicions about itself (more "slow member" death declarations with LHM on).
- Ruling: no TCP fallback probe and no TCP push-pull (memberlist has both) — the brief asks for UDP or a simulated network; one transport keeps the simulator faithful — large membership tables are sent as several self-contained UDP chunks instead, so a lost chunk only delays convergence.
- Ruling: the Node owns no goroutines; all progress comes from packets and from timers on an injected `clock.Clock` whose AfterFunc never runs synchronously — this is what makes the same code deterministic in `sim` and correct on the wall clock — user callbacks must not assume they run on a particular goroutine; ordering across concurrent entry points is not total on the real clock (documented on `Config.Events`).
- Ruling: membership event callbacks run after the node's lock is released — consumers (the cache) may call back into the node without deadlocks — callbacks from concurrent real-clock timers can interleave, so the cache re-reads the member's current state instead of trusting the event payload.

## Simulator

- Ruling: single-threaded discrete-event simulator with a (time, sequence) ordered heap and PCG streams derived from one seed; protocol code never iterates Go maps where order matters — Go map iteration is randomised, so any such loop would break bit-for-bit replay (`TestDeterministicReplay` guards this) — none.
- Ruling: "slow member" is modelled as inbound processing lag with on-time timers (`Host.SetLag`), separate from timer lag (`SetTimerLag`) — a uniform lag on both cancels out (the node's own timeout fires equally late), while a starved receive path is the case where a node's timeouts fire before it reads replies, i.e. the case Lifeguard was designed for — results for other slowness models (CPU-bound timers, bursty pauses) are not measured.
- Ruling: clusters are bootstrapped over a lossless link and loss is switched on afterwards — every trial then starts from the same converged membership, so measurements isolate steady-state behaviour from join dynamics — join behaviour under loss is only exercised by unit tests, not measured.

## Ring and cache

- Ruling: FNV-1a 64 followed by the splitmix64 finaliser as the ring hash — stable across processes (unlike `hash/maphash`), dependency-free, and the finaliser fixes FNV's weak avalanche on keys like `node#0`, `node#1` — not a keyed hash, so an adversary choosing keys could unbalance the ring.
- Ruling: immutable rings swapped through an atomic pointer — lookups never take a lock; Add is a sort of the new points plus a linear merge — every membership change allocates a new point slice (≈ 200 µs for 100 nodes × 256 vnodes, measured by `BenchmarkAddNode`).
- Ruling: suspect members stay on the ring; only Dead/Left members are removed — a transient suspicion (frequent under loss) should not shuffle data — requests to a really-dead suspect time out until it is declared dead.
- Ruling: last-writer-wins on a Lamport counter with the coordinator's name as tie-breaker; deletes are tombstones kept for `TombstoneTTL` — simple, deterministic and enough for a cache — concurrent writes to one key from different coordinators keep one value arbitrarily (no siblings / vector clocks); a replica offline for longer than the TTL can resurrect a deleted key.
- Ruling: when fewer replicas exist than the quorum (cluster smaller than N), the quorum shrinks to the number of replicas — a 1- or 2-node development cluster stays usable — no quorum guarantee in tiny clusters.
- Ruling: hand-off on membership change pushes keys only to replicas new since the last settled ring; non-owned keys are sent to the full replica set and dropped after a write quorum acks; a periodic ownership sweep (5 s) repeats the latter — found by a failing test: peers with a momentarily stale ring view pushed copies back to a node that had already handed them off — orphan copies can linger for one sweep interval.
- Ruling: blocking `Put/Get/Delete` wrappers exist only for real-clock use; the async forms are the primitive — a blocking call inside the simulator would deadlock because virtual time only advances when the caller runs the simulator — two API forms to document.

## Tooling

- Ruling: errcheck is configured to ignore `fmt.Fprint*` — those calls write human-readable output to an `io.Writer` (CLI stdout, buffers, hashes) where there is no useful recovery — a failing stdout write in the CLI goes unnoticed.
- Ruling: the coverage gate counts every package except `cmd/swimring` — the CLI is exercised by smoke tests but is presentation code — CLI regressions are caught only by those smoke tests.
- Ruling: `go.mod` declares go 1.24 (oldest version with `testing.B.Loop` and `slog.DiscardHandler`) while CI uses Go 1.25.x and development used 1.27 — widest compatibility — features newer than 1.24 cannot be used.
- Ruling: no `make integration` target — nothing here needs Docker; the UDP tests use only the loopback interface and run inside `make check` — `make check` fails on a machine where 127.0.0.1 UDP sockets cannot be opened (the transport test skips, the CLI demo tests do not).
