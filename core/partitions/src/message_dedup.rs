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

//! Per-partition message deduplication keyed on a user header.
//!
//! A topic opts in with `dedup_window` + `dedup_header`. Within the window,
//! the first message carrying a given header value is appended and later ones
//! are dropped. This is content deduplication, separate from the
//! `(client, request)` replay absorption in [`consensus::ClientTable`]: it
//! also catches resends from a new session, a restarted producer, or a second
//! producer publishing the same event.
//!
//! # Where decisions happen
//!
//! Only the partition primary decides, immediately before a request becomes
//! a prepare. Dropped messages are removed from the batch there, so the
//! decision is part of the replicated payload and backups apply it without
//! consulting any index. A mixed-version or misconfigured group can therefore
//! dedup less, but its logs can never fork.
//!
//! # Committed and pending
//!
//! The index has two halves. Committed keys are written only from committed
//! content: every replica folds each op's batch as the commit walk passes
//! it, and a rebuild reads committed storage back. Replicas at the same
//! commit therefore hold the same committed keys, and a backup promoted by a
//! view change decides exactly as its predecessor would have. Only a
//! committed key inside the window ever drops a message.
//!
//! Pending keys are those of appended but uncommitted batches, from any
//! append path (primary mint, backup receive, journal repair, WAL replay),
//! tagged with the op that appended them. A message matching one is not known
//! to be a duplicate: its original can still be truncated by a view change.
//! The primary refuses such a request as a whole with `TransientNotCommitted`
//! and the client retries it, by which time the original has committed (the
//! retry is dropped) or been truncated (the retry is appended). Nothing is
//! dropped on the strength of a pending key, so a pending key that outlives
//! its log entry costs a retry, never a message: truncation rolls pending
//! keys back, and when an op commits every pending key at or below it is
//! swept, which also clears any a rollback path failed to report.
//!
//! Keys are 128-bit hashes of the header value.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};

use iggy_binary_protocol::{WireUserHeaderIterator, validate_user_headers};
use iggy_common::{DedupHeaderName, MessageDedupPolicy};
use server_common::send_messages::{BatchIntegrity, BatchRef, decode_batch_slice_with};
use twox_hash::XxHash3_128;

/// Default cap on distinct keys held per partition.
///
/// About 64 bytes per entry, so the default bounds one partition's index near
/// 64 MiB. Past the cap the oldest entries are evicted even inside the
/// window; that is counted as saturation, because it means the window is not
/// fully enforced.
pub const DEFAULT_MESSAGE_DEDUP_ENTRIES_MAX: usize = 1_000_000;

/// Op recorded for entries rebuilt from committed storage, which does not
/// store ops. Such entries are committed by construction and never truncated.
pub const REBUILT_OP: u64 = 0;

/// The identity of one message under a topic's dedup policy: a 128-bit hash
/// of the configured header's value kind and bytes. Two distinct identities
/// collide with probability ~2^-128 per pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DedupKey(u128);

impl DedupKey {
    #[must_use]
    pub const fn from_raw(raw: u128) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_raw(self) -> u128 {
        self.0
    }
}

/// Extract the dedup key of one message from its raw user-header block.
///
/// `None` when the block is malformed or does not carry `header`: such a
/// message is admitted and not indexed. The block is validated before it is
/// walked because the send path does not validate user headers and the TLV
/// iterator panics on garbage.
#[must_use]
pub fn dedup_key(user_headers: &[u8], header: &DedupHeaderName) -> Option<DedupKey> {
    if user_headers.is_empty() || validate_user_headers(user_headers).is_err() {
        return None;
    }
    WireUserHeaderIterator::new(user_headers)
        .find(|entry| entry.key == header.as_bytes())
        .map(|entry| {
            let mut hasher = XxHash3_128::new();
            hasher.write(&[entry.value_kind.0]);
            hasher.write(entry.value);
            DedupKey(hasher.finish_128())
        })
}

/// One indexed occurrence of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupEntry {
    /// Absolute offset of the message holding the key.
    pub offset: u64,
    /// Broker timestamp of the batch that appended it (the prepare's
    /// timestamp), in microseconds.
    pub timestamp: u64,
    /// Op that appended it, or [`REBUILT_OP`] when read back from storage.
    pub op: u64,
    seq: u64,
}

/// Counters a partition drains into shard metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DedupCounters {
    /// Messages removed from requests because their key was committed and
    /// inside the window.
    pub dropped: u64,
    /// Requests refused transiently because a key matched an uncommitted
    /// occurrence.
    pub deferred: u64,
    /// Committed entries evicted by the entry cap while still inside the
    /// window.
    pub evicted_live: u64,
    /// Committed ops whose batch could not be read back at commit, so their
    /// keys were not indexed.
    pub unconfirmed_commits: u64,
}

/// How a key matched the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupMatch {
    /// A committed occurrence inside the window: the message is a duplicate.
    Committed,
    /// An appended occurrence that is not committed yet: whether the message
    /// is a duplicate is not known until it commits or is truncated.
    Uncommitted,
}

/// The per-partition dedup index. See the module docs for its invariants.
///
/// Two halves. `committed` holds keys of committed messages and is written
/// only from committed content (the commit walk and a rebuild from committed
/// storage), so replicas at the same commit hold the same set and only it
/// ever justifies dropping a message. `pending` holds keys appended but not
/// yet committed, tagged with their op, and only ever defers a request: it is
/// rolled back by truncation and swept when its op commits, which also
/// removes any occurrence a rollback path failed to report.
#[derive(Debug)]
pub struct MessageDedupIndex {
    committed: HashMap<DedupKey, DedupEntry, BuildHasherDefault<KeyHasher>>,
    /// Insertion order of `committed`, for eviction. An item whose seq no
    /// longer matches the live entry is stale and skipped.
    order: VecDeque<(u64, DedupKey)>,
    pending: HashMap<DedupKey, DedupEntry, BuildHasherDefault<KeyHasher>>,
    pending_by_op: BTreeMap<u64, Vec<(DedupKey, u64)>>,
    next_seq: u64,
    /// Newest committed timestamp folded in. Physical eviction is measured
    /// against it, never against a local clock.
    newest_timestamp: u64,
    entries_max: usize,
    counters: DedupCounters,
}

impl MessageDedupIndex {
    #[must_use]
    pub fn new(entries_max: usize) -> Self {
        Self {
            committed: HashMap::default(),
            order: VecDeque::new(),
            pending: HashMap::default(),
            pending_by_op: BTreeMap::new(),
            next_seq: 0,
            newest_timestamp: 0,
            entries_max: entries_max.max(1),
            counters: DedupCounters::default(),
        }
    }

    /// Committed keys held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.committed.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.committed.is_empty()
    }

    /// Uncommitted keys held.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub const fn entries_max(&self) -> usize {
        self.entries_max
    }

    pub fn set_entries_max(&mut self, entries_max: usize, window_micros: u64) {
        self.entries_max = entries_max.max(1);
        self.evict(window_micros);
    }

    #[must_use]
    pub fn committed_entry(&self, key: DedupKey) -> Option<&DedupEntry> {
        self.committed.get(&key)
    }

    #[must_use]
    pub fn pending_entry(&self, key: DedupKey) -> Option<&DedupEntry> {
        self.pending.get(&key)
    }

    /// How `key` matches as of `now`: a committed occurrence inside the
    /// window, else an uncommitted occurrence above `commit_op`.
    ///
    /// A pending occurrence at or below `commit_op` can no longer commit: its
    /// op already did, with content the commit walk has either indexed or
    /// could not read. It is ignored rather than trusted either way. Treating
    /// it as uncommitted would defer the key until a later commit sweeps it,
    /// and when every request in flight carries such a key nothing commits,
    /// so nothing would ever sweep it.
    #[must_use]
    pub fn lookup(
        &self,
        key: DedupKey,
        now: u64,
        window_micros: u64,
        commit_op: u64,
    ) -> Option<DedupMatch> {
        if self
            .committed
            .get(&key)
            .is_some_and(|entry| entry.timestamp.saturating_add(window_micros) > now)
        {
            return Some(DedupMatch::Committed);
        }
        self.pending
            .get(&key)
            .is_some_and(|entry| entry.op > commit_op)
            .then_some(DedupMatch::Uncommitted)
    }

    /// Note an appended, uncommitted occurrence of `key`.
    ///
    /// Re-recording the same op updates the entry in place: the primary
    /// records at admission with a provisional offset, then again at append
    /// with the stamped one.
    pub fn record_pending(&mut self, key: DedupKey, offset: u64, timestamp: u64, op: u64) {
        if let Some(existing) = self.pending.get_mut(&key)
            && existing.op == op
        {
            existing.offset = offset;
            existing.timestamp = timestamp;
            return;
        }
        let seq = self.next_seq();
        self.pending.insert(
            key,
            DedupEntry {
                offset,
                timestamp,
                op,
                seq,
            },
        );
        self.pending_by_op.entry(op).or_default().push((key, seq));
    }

    /// Index a committed occurrence of `key`. Only committed content may
    /// reach this: the commit walk, or a rebuild from committed storage.
    pub fn record_committed(
        &mut self,
        key: DedupKey,
        offset: u64,
        timestamp: u64,
        op: u64,
        window_micros: u64,
    ) {
        self.newest_timestamp = self.newest_timestamp.max(timestamp);
        if let Some(existing) = self.committed.get_mut(&key)
            && existing.offset == offset
        {
            if existing.op == REBUILT_OP {
                existing.op = op;
            }
            return;
        }
        let seq = self.next_seq();
        self.committed.insert(
            key,
            DedupEntry {
                offset,
                timestamp,
                op,
                seq,
            },
        );
        self.order.push_back((seq, key));
        self.evict(window_micros);
    }

    /// `op` committed: drop every uncommitted occurrence at or below it. The
    /// committed ones were just indexed from the committed content, so what is
    /// left is either that same content (now redundant) or an occurrence from
    /// a log suffix this replica lost without a reported truncation.
    pub fn sweep_committed_through(&mut self, op: u64) {
        let remaining = self.pending_by_op.split_off(&(op + 1));
        let swept = std::mem::replace(&mut self.pending_by_op, remaining);
        for (key, seq) in swept.into_values().flatten() {
            if self.pending.get(&key).is_some_and(|entry| entry.seq == seq) {
                self.pending.remove(&key);
            }
        }
    }

    /// Forget every uncommitted occurrence appended by `from_op` or later,
    /// mirroring a truncation of the uncommitted log suffix. Committed
    /// entries are never truncated.
    pub fn truncate_from_op(&mut self, from_op: u64) {
        let truncated = self.pending_by_op.split_off(&from_op);
        for (key, seq) in truncated.into_values().flatten() {
            if self.pending.get(&key).is_some_and(|entry| entry.seq == seq) {
                self.pending.remove(&key);
            }
        }
    }

    pub fn clear(&mut self) {
        self.committed.clear();
        self.order.clear();
        self.pending.clear();
        self.pending_by_op.clear();
        self.newest_timestamp = 0;
    }

    #[must_use]
    pub const fn counters(&self) -> DedupCounters {
        self.counters
    }

    /// Return the counters accumulated since the last call and reset them.
    pub fn take_counters(&mut self) -> DedupCounters {
        std::mem::take(&mut self.counters)
    }

    pub const fn note_dropped(&mut self, dropped: u64) {
        self.counters.dropped = self.counters.dropped.saturating_add(dropped);
    }

    pub const fn note_deferred(&mut self) {
        self.counters.deferred = self.counters.deferred.saturating_add(1);
    }

    pub const fn note_unconfirmed_commit(&mut self) {
        self.counters.unconfirmed_commits = self.counters.unconfirmed_commits.saturating_add(1);
    }

    /// Order-independent digest of the committed keys (key, offset,
    /// timestamp). Replicas at the same commit hold the same committed set.
    #[must_use]
    pub fn fingerprint(&self) -> u128 {
        let mut items: Vec<(u128, u64, u64)> = self
            .committed
            .iter()
            .map(|(key, entry)| (key.0, entry.offset, entry.timestamp))
            .collect();
        items.sort_unstable();
        let mut hasher = XxHash3_128::new();
        for (key, offset, timestamp) in items {
            hasher.write(&key.to_le_bytes());
            hasher.write(&offset.to_le_bytes());
            hasher.write(&timestamp.to_le_bytes());
        }
        hasher.finish_128()
    }

    /// Committed entries as `(key, offset, timestamp, op)`, sorted.
    /// Diagnostics only.
    #[must_use]
    pub fn entries_sorted(&self) -> Vec<(u128, u64, u64, u64)> {
        let mut items: Vec<(u128, u64, u64, u64)> = self
            .committed
            .iter()
            .map(|(key, entry)| (key.0, entry.offset, entry.timestamp, entry.op))
            .collect();
        items.sort_unstable();
        items
    }

    const fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn evict(&mut self, window_micros: u64) {
        let cutoff = self.newest_timestamp.saturating_sub(window_micros);
        while let Some(&(seq, key)) = self.order.front() {
            match self.committed.get(&key) {
                Some(entry) if entry.seq == seq => {
                    if entry.timestamp >= cutoff {
                        break;
                    }
                    self.committed.remove(&key);
                    self.order.pop_front();
                }
                _ => {
                    self.order.pop_front();
                }
            }
        }
        while self.committed.len() > self.entries_max {
            let Some((seq, key)) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.committed.get(&key)
                && entry.seq == seq
            {
                if entry.timestamp >= cutoff {
                    self.counters.evicted_live = self.counters.evicted_live.saturating_add(1);
                }
                self.committed.remove(&key);
            }
        }
        self.compact_order_if_sparse();
    }

    /// Stale queue items are left behind by replacement. Keep them from
    /// outgrowing the live entries so memory stays proportional to the index,
    /// not to its history.
    fn compact_order_if_sparse(&mut self) {
        if self.order.len() <= 64 || self.order.len() <= self.committed.len().saturating_mul(2) {
            return;
        }
        let committed = &self.committed;
        self.order
            .retain(|(seq, key)| committed.get(key).is_some_and(|entry| entry.seq == *seq));
    }
}

/// What the primary should do with one `SendMessages` request.
#[derive(Debug, PartialEq, Eq)]
pub struct BatchVerdict {
    /// Per message, whether it is kept.
    pub keep: Vec<bool>,
    /// Keys of the kept messages, in kept order (`None` for a kept message
    /// without a key).
    pub kept_keys: Vec<Option<DedupKey>>,
    /// Messages dropped: committed duplicates, and repeats earlier in the
    /// same batch.
    pub dropped: u64,
    /// Whether any message matched an uncommitted occurrence. Such a request
    /// is refused transiently as a whole, never filtered.
    pub matched_uncommitted: bool,
}

impl BatchVerdict {
    #[must_use]
    pub const fn keeps_all(&self) -> bool {
        self.dropped == 0
    }

    #[must_use]
    pub const fn keeps_none(&self) -> bool {
        self.kept_keys.is_empty()
    }
}

/// Decide which messages of `batch` are duplicates.
///
/// A message is dropped when its key has a committed occurrence inside the
/// window as of `now`, or when an earlier message of the same batch carried
/// the same key (first wins). A match against an uncommitted occurrence is
/// only flagged: the caller defers the whole request.
#[must_use]
pub fn classify_batch(
    batch: &BatchRef<'_>,
    policy: &MessageDedupPolicy,
    index: &MessageDedupIndex,
    now: u64,
    commit_op: u64,
) -> BatchVerdict {
    let window = policy.window_micros();
    let count = batch.header.message_count as usize;
    let mut verdict = BatchVerdict {
        keep: Vec::with_capacity(count),
        kept_keys: Vec::with_capacity(count),
        dropped: 0,
        matched_uncommitted: false,
    };
    let mut seen_in_batch: HashSet<DedupKey, BuildHasherDefault<KeyHasher>> = HashSet::default();
    for view in batch {
        let Some(key) = dedup_key(view.user_headers, &policy.header) else {
            verdict.keep.push(true);
            verdict.kept_keys.push(None);
            continue;
        };
        if seen_in_batch.contains(&key) {
            verdict.keep.push(false);
            verdict.dropped += 1;
            continue;
        }
        match index.lookup(key, now, window, commit_op) {
            Some(DedupMatch::Committed) => {
                verdict.keep.push(false);
                verdict.dropped += 1;
                continue;
            }
            Some(DedupMatch::Uncommitted) => verdict.matched_uncommitted = true,
            None => {}
        }
        seen_in_batch.insert(key);
        verdict.keep.push(true);
        verdict.kept_keys.push(Some(key));
    }
    verdict
}

/// Keys of an appended, stamped batch record (`[batch header][frames]`) with
/// their absolute offsets and the batch timestamp, as every replica reads
/// them from the same stamped bytes.
#[must_use]
pub fn stamped_batch_keys(policy: &MessageDedupPolicy, body: &[u8]) -> Vec<(DedupKey, u64, u64)> {
    let Ok(batch) = decode_batch_slice_with(body, BatchIntegrity::LayoutOnly) else {
        return Vec::new();
    };
    let base_offset = batch.header.base_offset;
    let timestamp = batch.header.base_timestamp;
    batch
        .iter()
        .filter_map(|view| {
            dedup_key(view.user_headers, &policy.header).map(|key| {
                (
                    key,
                    base_offset + u64::from(view.header.offset_delta),
                    timestamp,
                )
            })
        })
        .collect()
}

/// Fold an appended, uncommitted batch into the pending half.
pub fn record_pending_batch(
    index: &mut MessageDedupIndex,
    policy: &MessageDedupPolicy,
    body: &[u8],
    op: u64,
) {
    for (key, offset, timestamp) in stamped_batch_keys(policy, body) {
        index.record_pending(key, offset, timestamp, op);
    }
}

/// Fold a committed batch into the committed half and sweep the pending
/// occurrences it supersedes.
pub fn record_committed_batch(
    index: &mut MessageDedupIndex,
    policy: &MessageDedupPolicy,
    body: &[u8],
    op: u64,
) {
    let window = policy.window_micros();
    for (key, offset, timestamp) in stamped_batch_keys(policy, body) {
        index.record_committed(key, offset, timestamp, op, window);
    }
    index.sweep_committed_through(op);
}

/// Keys are already uniformly distributed hashes; feed their low 64 bits to
/// the map directly instead of hashing a hash.
#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(*byte);
        }
    }

    fn write_u128(&mut self, value: u128) {
        // Truncation is the point: the low half of a uniform hash is uniform.
        #[allow(clippy::cast_possible_truncation)]
        {
            self.0 = value as u64;
        }
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use std::str::FromStr;

    use bytes::BytesMut;
    use iggy_binary_protocol::encode_user_headers;
    use iggy_common::IggyDuration;

    use super::*;

    const WINDOW: u64 = 1_000;
    const STRING_KIND: u8 = 2;
    const RAW_KIND: u8 = 1;

    fn key(value: u128) -> DedupKey {
        DedupKey::from_raw(value)
    }

    fn policy(header: &str) -> MessageDedupPolicy {
        MessageDedupPolicy {
            window: IggyDuration::from(WINDOW),
            header: DedupHeaderName::from_str(header).unwrap(),
        }
    }

    fn headers(entries: &[(&str, u8, &[u8])]) -> Vec<u8> {
        let encoded: Vec<(u8, &[u8], u8, &[u8])> = entries
            .iter()
            .map(|(name, kind, value)| (STRING_KIND, name.as_bytes(), *kind, *value))
            .collect();
        let mut buffer = BytesMut::new();
        encode_user_headers(&encoded, &mut buffer);
        buffer.to_vec()
    }

    #[test]
    fn key_is_read_from_the_configured_header_only() {
        let block = headers(&[
            ("trace", STRING_KIND, b"t-1"),
            ("dedup-key", STRING_KIND, b"evt-1"),
        ]);
        let name = DedupHeaderName::from_str("dedup-key").unwrap();
        let found = dedup_key(&block, &name).expect("header present");
        let other = headers(&[("dedup-key", STRING_KIND, b"evt-2")]);
        assert_ne!(Some(found), dedup_key(&other, &name));
        // Same value under another header name is a different policy's key.
        let trace = DedupHeaderName::from_str("trace").unwrap();
        assert_ne!(dedup_key(&block, &trace), Some(found));
    }

    #[test]
    fn same_value_is_the_same_key_across_batches_and_header_order() {
        let name = DedupHeaderName::from_str("dedup-key").unwrap();
        let first = headers(&[("a", STRING_KIND, b"x"), ("dedup-key", STRING_KIND, b"evt")]);
        let second = headers(&[("dedup-key", STRING_KIND, b"evt"), ("b", STRING_KIND, b"y")]);
        assert_eq!(dedup_key(&first, &name), dedup_key(&second, &name));
    }

    #[test]
    fn value_kind_is_part_of_the_identity() {
        // `evt` as a string and as raw bytes are different typed values; a
        // producer that switches encodings mid-stream is not deduplicated
        // against itself, which is documented rather than guessed at.
        let name = DedupHeaderName::from_str("dedup-key").unwrap();
        let as_string = headers(&[("dedup-key", STRING_KIND, b"evt")]);
        let as_raw = headers(&[("dedup-key", RAW_KIND, b"evt")]);
        assert_ne!(dedup_key(&as_string, &name), dedup_key(&as_raw, &name));
    }

    #[test]
    fn missing_empty_or_malformed_headers_yield_no_key_and_never_panic() {
        let name = DedupHeaderName::from_str("dedup-key").unwrap();
        assert_eq!(dedup_key(&[], &name), None);
        assert_eq!(
            dedup_key(&headers(&[("other", STRING_KIND, b"v")]), &name),
            None
        );
        // Truncated, zero kind, overlong length, odd TLV count.
        let mut garbage: Vec<Vec<u8>> = vec![vec![2, 9, 0, 0, 0, b'd'], vec![0; 12]];
        let mut odd = headers(&[("dedup-key", STRING_KIND, b"evt")]);
        odd.truncate(odd.len() - 1);
        garbage.push(odd);
        garbage.push(vec![2, 0xff, 0xff, 0xff, 0x7f]);
        for block in garbage {
            assert_eq!(dedup_key(&block, &name), None, "{block:?}");
        }
    }

    #[test]
    fn committed_keys_match_inside_the_window_only() {
        let mut index = MessageDedupIndex::new(16);
        index.record_committed(key(1), 10, 5_000, 3, WINDOW);
        assert_eq!(
            index.lookup(key(1), 5_999, WINDOW, 0),
            Some(DedupMatch::Committed)
        );
        assert_eq!(
            index.lookup(key(1), 6_000, WINDOW, 0),
            None,
            "window is half-open"
        );
        assert_eq!(index.lookup(key(2), 5_000, WINDOW, 0), None);
    }

    #[test]
    fn pending_keys_only_ever_defer() {
        let mut index = MessageDedupIndex::new(16);
        index.record_pending(key(1), 0, 5_000, 7);
        assert_eq!(
            index.lookup(key(1), 5_000, WINDOW, 0),
            Some(DedupMatch::Uncommitted)
        );
        // A committed occurrence outranks a pending one of the same key.
        index.record_committed(key(1), 0, 5_000, 7, WINDOW);
        assert_eq!(
            index.lookup(key(1), 5_000, WINDOW, 0),
            Some(DedupMatch::Committed)
        );
    }

    #[test]
    fn commit_sweeps_pending_keys_at_or_below_its_op_including_unreported_ones() {
        // Op 4 appended key 1 on this replica, but a view change replaced op 4
        // without this replica hearing of the truncation. When the canonical
        // op 4 (key 2) commits, key 1's stale occurrence must go: left behind,
        // it would defer every retry of an event that never committed.
        let mut index = MessageDedupIndex::new(16);
        index.record_pending(key(1), 10, 1_000, 4);
        index.record_pending(key(3), 11, 1_000, 5);
        index.record_committed(key(2), 10, 1_100, 4, WINDOW);
        index.sweep_committed_through(4);
        assert_eq!(index.lookup(key(1), 1_100, WINDOW, 0), None);
        assert_eq!(
            index.lookup(key(2), 1_100, WINDOW, 0),
            Some(DedupMatch::Committed)
        );
        assert_eq!(
            index.lookup(key(3), 1_100, WINDOW, 0),
            Some(DedupMatch::Uncommitted)
        );
    }

    #[test]
    fn a_pending_key_at_or_below_the_commit_point_is_ignored_not_deferred() {
        // Replayed at boot for an op that already committed, and never swept
        // because nothing has committed since. Deferring on it would block
        // the very commits that would sweep it.
        let mut index = MessageDedupIndex::new(16);
        index.record_pending(key(1), 0, 100, 5);
        assert_eq!(
            index.lookup(key(1), 100, WINDOW, 4),
            Some(DedupMatch::Uncommitted)
        );
        assert_eq!(index.lookup(key(1), 100, WINDOW, 5), None);
        assert_eq!(index.lookup(key(1), 100, WINDOW, 9), None);
    }

    #[test]
    fn truncation_rolls_back_pending_keys_and_never_committed_ones() {
        let mut index = MessageDedupIndex::new(16);
        index.record_committed(key(1), 0, 100, 3, WINDOW);
        index.record_pending(key(2), 1, 110, 4);
        index.record_pending(key(3), 2, 120, 5);
        index.record_pending(key(4), 3, 130, 6);
        index.truncate_from_op(5);
        assert_eq!(
            index.lookup(key(1), 130, WINDOW, 0),
            Some(DedupMatch::Committed)
        );
        assert_eq!(
            index.lookup(key(2), 130, WINDOW, 0),
            Some(DedupMatch::Uncommitted)
        );
        assert_eq!(index.lookup(key(3), 130, WINDOW, 0), None);
        assert_eq!(index.lookup(key(4), 130, WINDOW, 0), None);
        // Truncating below a committed op leaves it alone.
        index.truncate_from_op(1);
        assert_eq!(
            index.lookup(key(1), 130, WINDOW, 0),
            Some(DedupMatch::Committed)
        );
        assert_eq!(index.pending_len(), 0);
    }

    #[test]
    fn re_recording_a_pending_op_takes_the_stamped_offset_in_place() {
        let mut index = MessageDedupIndex::new(16);
        index.record_pending(key(7), 100, 5_000, 9);
        index.record_pending(key(7), 104, 5_000, 9);
        assert_eq!(index.pending_len(), 1);
        assert_eq!(index.pending_entry(key(7)).unwrap().offset, 104);
        // A single sweep entry per recording, so the sweep cannot double-free.
        index.sweep_committed_through(9);
        assert_eq!(index.pending_len(), 0);
    }

    #[test]
    fn a_newer_pending_occurrence_is_not_swept_by_an_older_commit() {
        let mut index = MessageDedupIndex::new(16);
        index.record_pending(key(1), 0, 100, 2);
        index.record_pending(key(1), 5, 200, 6);
        index.sweep_committed_through(3);
        assert_eq!(index.pending_entry(key(1)).unwrap().op, 6);
    }

    #[test]
    fn eviction_follows_the_newest_committed_timestamp_not_a_clock() {
        let mut index = MessageDedupIndex::new(16);
        index.record_committed(key(1), 0, 1_000, 1, WINDOW);
        index.record_committed(key(2), 1, 1_500, 2, WINDOW);
        assert_eq!(index.len(), 2);
        index.record_committed(key(3), 2, 2_200, 3, WINDOW);
        assert!(index.committed_entry(key(1)).is_none());
        assert!(index.committed_entry(key(2)).is_some());
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn capacity_evicts_oldest_and_counts_live_evictions_as_saturation() {
        let mut index = MessageDedupIndex::new(3);
        for value in 0..5u128 {
            index.record_committed(
                key(value),
                value as u64,
                100 + value as u64,
                1 + value as u64,
                WINDOW,
            );
        }
        assert_eq!(index.len(), 3);
        assert!(index.committed_entry(key(0)).is_none() && index.committed_entry(key(1)).is_none());
        assert!((2..5).all(|value| index.committed_entry(key(value)).is_some()));
        assert_eq!(index.take_counters().evicted_live, 2);
        assert_eq!(index.counters().evicted_live, 0, "take resets");
    }

    #[test]
    fn rebuild_overlapping_the_commit_walk_keeps_one_entry_with_the_real_op() {
        let mut walked_first = MessageDedupIndex::new(16);
        walked_first.record_committed(key(7), 50, 5_000, REBUILT_OP, WINDOW);
        walked_first.record_committed(key(7), 50, 5_000, 12, WINDOW);
        assert_eq!(walked_first.committed_entry(key(7)).unwrap().op, 12);
        let mut committed_first = MessageDedupIndex::new(16);
        committed_first.record_committed(key(7), 50, 5_000, 12, WINDOW);
        committed_first.record_committed(key(7), 50, 5_000, REBUILT_OP, WINDOW);
        assert_eq!(committed_first.committed_entry(key(7)).unwrap().op, 12);
        assert_eq!(walked_first.fingerprint(), committed_first.fingerprint());
    }

    #[test]
    fn fingerprint_depends_on_committed_content_only() {
        // Two replicas at the same commit, one with an extra pending tail.
        let log: Vec<(u128, u64, u64, u64)> = (0..200u128)
            .map(|value| {
                (
                    value % 37,
                    value as u64,
                    1_000 + value as u64 * 7,
                    1 + value as u64 / 4,
                )
            })
            .collect();
        let mut ahead = MessageDedupIndex::new(1_000);
        let mut behind = MessageDedupIndex::new(1_000);
        for (value, offset, timestamp, op) in &log {
            ahead.record_pending(key(*value), offset + 1_000, *timestamp, *op);
            ahead.record_committed(key(*value), *offset, *timestamp, *op, WINDOW);
            behind.record_committed(key(*value), *offset, *timestamp, *op, WINDOW);
        }
        ahead.record_pending(key(999), 5_000, 9_999, 999);
        assert_eq!(ahead.fingerprint(), behind.fingerprint());
        assert_eq!(ahead.len(), behind.len());
    }

    #[test]
    fn stale_order_items_do_not_grow_without_bound() {
        let mut index = MessageDedupIndex::new(8);
        for round in 0..10_000u64 {
            index.record_committed(key(u128::from(round % 4)), round, 10_000, round + 1, WINDOW);
        }
        assert_eq!(index.len(), 4);
        assert!(
            index.order.len() <= 64.max(index.len() * 2) + 1,
            "{}",
            index.order.len()
        );
    }

    #[test]
    fn clear_resets_everything_including_the_eviction_reference() {
        let mut index = MessageDedupIndex::new(8);
        index.record_committed(key(1), 0, 50_000, 1, WINDOW);
        index.record_pending(key(3), 1, 50_000, 2);
        index.clear();
        assert!(index.is_empty() && index.pending_len() == 0);
        index.record_committed(key(2), 0, 100, 1, WINDOW);
        assert!(index.committed_entry(key(2)).is_some());
    }

    mod classify {
        use server_common::send_messages::{SendMessagesOwned, decode_batch_slice};

        use super::*;

        fn batch_bytes(keys: &[Option<&str>]) -> Vec<u8> {
            use bytes::Bytes;
            use server_common::send_messages::{IggyMessage, IggyMessageHeader, IggyMessages};
            let mut messages = IggyMessages::with_capacity(keys.len());
            for (index, value) in keys.iter().enumerate() {
                messages.push(IggyMessage {
                    header: IggyMessageHeader {
                        id: index as u128 + 1,
                        origin_timestamp: 1,
                        ..Default::default()
                    },
                    payload: Bytes::from(format!("payload-{index}")),
                    user_headers: value.map(|value| {
                        Bytes::from(headers(&[("dedup-key", STRING_KIND, value.as_bytes())]))
                    }),
                });
            }
            let owned = SendMessagesOwned::from_messages(
                server_common::sharding::IggyNamespace::new(1, 1, 0),
                &messages,
            )
            .unwrap();
            let mut out = vec![0u8; owned.header.total_size()];
            owned
                .header
                .encode_into(&mut out[..server_common::send_messages::BATCH_HEADER_SIZE]);
            out[server_common::send_messages::BATCH_HEADER_SIZE..].copy_from_slice(&owned.blob);
            out
        }

        fn classify(keys: &[Option<&str>], index: &MessageDedupIndex, now: u64) -> BatchVerdict {
            let bytes = batch_bytes(keys);
            let batch = decode_batch_slice(&bytes).unwrap();
            classify_batch(&batch, &policy("dedup-key"), index, now, 0)
        }

        fn key_of(value: &str) -> DedupKey {
            dedup_key(
                &headers(&[("dedup-key", STRING_KIND, value.as_bytes())]),
                &DedupHeaderName::from_str("dedup-key").unwrap(),
            )
            .unwrap()
        }

        #[test]
        fn fresh_batch_keeps_everything() {
            let verdict = classify(&[Some("a"), None, Some("b")], &MessageDedupIndex::new(8), 0);
            assert!(verdict.keeps_all());
            assert_eq!(verdict.keep, vec![true, true, true]);
            assert_eq!(
                verdict.kept_keys,
                vec![Some(key_of("a")), None, Some(key_of("b"))]
            );
        }

        #[test]
        fn repeats_inside_one_batch_keep_the_first_only() {
            let verdict = classify(
                &[Some("a"), Some("a"), None, None, Some("a")],
                &MessageDedupIndex::new(8),
                0,
            );
            assert_eq!(verdict.keep, vec![true, false, true, true, false]);
            assert_eq!(verdict.dropped, 2);
            assert!(!verdict.matched_uncommitted);
        }

        #[test]
        fn committed_live_keys_are_dropped_and_expired_ones_readmitted() {
            let mut index = MessageDedupIndex::new(8);
            index.record_committed(key_of("old"), 0, 100, 1, WINDOW);
            index.record_committed(key_of("new"), 1, 900, 2, WINDOW);
            let verdict = classify(&[Some("old"), Some("new"), Some("fresh")], &index, 1_500);
            assert_eq!(verdict.keep, vec![true, false, true], "old expired at 1100");
            assert!(!verdict.matched_uncommitted);
        }

        #[test]
        fn a_pending_match_is_flagged_and_never_dropped() {
            // Dropping against an occurrence that can still be truncated could
            // lose the event; the caller defers the whole request instead.
            let mut index = MessageDedupIndex::new(8);
            index.record_pending(key_of("pending"), 5, 1_000, 9);
            index.record_committed(key_of("done"), 4, 1_000, 3, WINDOW);
            let verdict = classify(&[Some("pending"), Some("done"), Some("new")], &index, 1_000);
            assert!(verdict.matched_uncommitted);
            assert_eq!(verdict.keep, vec![true, false, true]);
        }

        #[test]
        fn committed_batches_use_stamped_offsets_and_timestamp_and_sweep_pending() {
            let mut bytes = batch_bytes(&[Some("a"), None, Some("b")]);
            let mut header = server_common::send_messages::BatchHeader::decode(&bytes).unwrap();
            header.base_offset = 40;
            header.base_timestamp = 7_000;
            header.encode_into(&mut bytes[..server_common::send_messages::BATCH_HEADER_SIZE]);
            let mut index = MessageDedupIndex::new(8);
            record_pending_batch(&mut index, &policy("dedup-key"), &bytes, 11);
            assert_eq!(index.pending_len(), 2);
            record_committed_batch(&mut index, &policy("dedup-key"), &bytes, 11);
            let a = index.committed_entry(key_of("a")).unwrap();
            let b = index.committed_entry(key_of("b")).unwrap();
            assert_eq!((a.offset, a.timestamp, a.op), (40, 7_000, 11));
            assert_eq!((b.offset, b.timestamp, b.op), (42, 7_000, 11));
            assert_eq!((index.len(), index.pending_len()), (2, 0));
        }
    }
}
