//! Window identity (brief §2.B §3.D). This module will also hold the one
//! replay rule, `replay_plan`; for now it holds the identity every window
//! carries to its applies.

/// Where a window was drained from and to. `start` is the watermark the drain
/// began at — the one position a re-drain of the same window reproduces —
/// and `end` is the last complete commit it read, the only valid next
/// watermark. A re-drain recomputes `end`, so only `start` can stamp a row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WindowId {
    start: u64,
    end: u64,
}

impl WindowId {
    pub(crate) fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// The watermark every apply of this window hands its unit's close.
    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    /// The changelog stamp (`_apitap_lsn`). Read by the ClickHouse and
    /// BigQuery changelog applies until `replay_plan` owns the stamp.
    pub(crate) fn start(&self) -> u64 {
        self.start
    }
}
