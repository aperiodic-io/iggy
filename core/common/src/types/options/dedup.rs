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

use std::fmt;
use std::str::FromStr;

use crate::IggyDuration;

/// Longest header key a topic may dedup on. Wire header keys go up to 255
/// bytes; the cap keeps [`DedupHeaderName`] a small `Copy` value so
/// [`super::TopicRuntimeOptions`] stays `Copy` for the partition hot path.
pub const MAX_DEDUP_HEADER_LENGTH: usize = 64;

/// Longest dedup window a topic may configure. The index costs memory per
/// distinct key seen inside the window, so an unbounded window is an
/// unbounded index; a day is far past any producer retry horizon.
pub const MAX_DEDUP_WINDOW_MICROS: u64 = 24 * 60 * 60 * 1_000_000;

/// The user-header key whose value identifies a message for deduplication.
///
/// Matched against header keys byte for byte, regardless of the key's
/// declared kind: producers in different SDKs encode the same text key as
/// `String` or `Raw`, and both must dedup together.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DedupHeaderName {
    bytes: [u8; MAX_DEDUP_HEADER_LENGTH],
    len: u8,
}

/// Why a string cannot name a dedup header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupHeaderNameError {
    Empty,
    TooLong,
}

impl DedupHeaderName {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl FromStr for DedupHeaderName {
    type Err = DedupHeaderNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let raw = value.as_bytes();
        if raw.is_empty() {
            return Err(DedupHeaderNameError::Empty);
        }
        if raw.len() > MAX_DEDUP_HEADER_LENGTH {
            return Err(DedupHeaderNameError::TooLong);
        }
        let mut bytes = [0u8; MAX_DEDUP_HEADER_LENGTH];
        bytes[..raw.len()].copy_from_slice(raw);
        Ok(Self {
            bytes,
            len: u8::try_from(raw.len()).map_err(|_| DedupHeaderNameError::TooLong)?,
        })
    }
}

impl fmt::Display for DedupHeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(self.as_bytes()))
    }
}

impl fmt::Debug for DedupHeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DedupHeaderName({self})")
    }
}

/// A topic's resolved message-deduplication policy.
///
/// Present only when the topic set a non-zero `dedup_window` and a
/// `dedup_header`: a partition either dedups with both or not at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageDedupPolicy {
    pub window: IggyDuration,
    pub header: DedupHeaderName,
}

impl MessageDedupPolicy {
    #[must_use]
    pub fn window_micros(&self) -> u64 {
        self.window.as_micros()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_name_round_trips_and_rejects_bad_lengths() {
        let name = DedupHeaderName::from_str("dedup-key").unwrap();
        assert_eq!(name.as_bytes(), b"dedup-key");
        assert_eq!(name.to_string(), "dedup-key");
        assert_eq!(
            DedupHeaderName::from_str(""),
            Err(DedupHeaderNameError::Empty)
        );
        let longest = "k".repeat(MAX_DEDUP_HEADER_LENGTH);
        assert!(DedupHeaderName::from_str(&longest).is_ok());
        assert_eq!(
            DedupHeaderName::from_str(&format!("{longest}k")),
            Err(DedupHeaderNameError::TooLong)
        );
    }

    #[test]
    fn header_names_compare_by_content_not_padding() {
        let short = DedupHeaderName::from_str("ab").unwrap();
        let other = DedupHeaderName::from_str("abc").unwrap();
        assert_ne!(short, other);
        assert_eq!(short, DedupHeaderName::from_str("ab").unwrap());
    }
}
