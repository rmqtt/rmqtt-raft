//! A sliding window timeout recorder for counting and calculating event rates.
//!
//! `TimeoutRecorder` splits a fixed time window into multiple equal-sized buckets.
//! Each event is recorded into the corresponding bucket based on the current time.
//!
//! This allows efficient computation of:
//! - Total number of recent events within the window (`get()`)
//! - Average event rate (`rate_per_second()`, `rate_per()`)
//! - Recent activity in the current bucket (`recent_get()`, `recent_bucket_rate_per_second()`)
//!
//! This structure is useful for rate-limiting, monitoring, and analytics where
//! time-based metrics are required without storing every individual event.
//!
//! ## Example
//! ```rust,ignore
//! let mut recorder = TimeoutRecorder::new(std::time::Duration::from_secs(10), 10);
//! recorder.incr(); // record an event
//! let total = recorder.get(); // get total in last 10 seconds
//! let rate = recorder.rate_per_second(); // average rate per second
//! ```

use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct TimeoutRecorder {
    buckets: Vec<Bucket>,
    window_size: Duration,
    bucket_count: usize,
    bucket_duration: Duration,
    max: usize,
}

#[derive(Debug, Clone)]
struct Bucket {
    timestamp: Instant,
    count: usize,
}

impl TimeoutRecorder {
    /// Creates a new TimeoutRecorder with the given time window and number of buckets.
    pub fn new(window_size: Duration, bucket_count: usize) -> Self {
        let now = Instant::now();
        let buckets = vec![
            Bucket {
                timestamp: now,
                count: 0,
            };
            bucket_count
        ];

        TimeoutRecorder {
            buckets,
            window_size,
            bucket_count,
            bucket_duration: window_size / bucket_count as u32,
            max: 0,
        }
    }

    /// Increments the count in the current bucket.
    #[inline]
    pub fn incr(&mut self) {
        let now = Instant::now();
        let index = self.bucket_index(now);

        let bucket = &mut self.buckets[index];
        let dur = now.duration_since(bucket.timestamp);
        if dur >= self.window_size || dur >= self.bucket_duration {
            // Reset the bucket if it's expired
            bucket.count = 0;
            bucket.timestamp = now;
        }

        bucket.count += 1;
        self.max += 1;
    }

    /// Returns the total count of events in the valid time window.
    #[inline]
    #[allow(dead_code)]
    pub fn get(&self) -> usize {
        let now = Instant::now();
        self.buckets
            .iter()
            .filter(|b| now.duration_since(b.timestamp) <= self.window_size)
            .map(|b| b.count)
            .sum()
    }

    /// Returns the maximum total count ever recorded.
    #[inline]
    pub fn max(&self) -> usize {
        self.max
    }

    /// Returns the average rate per second over the entire window.
    #[allow(dead_code)]
    #[inline]
    pub fn rate_per_second(&self) -> f64 {
        self.rate_per(Duration::from_secs(1))
    }

    /// Returns the average rate per the given duration over the window.
    #[allow(dead_code)]
    #[inline]
    pub fn rate_per(&self, per: Duration) -> f64 {
        let count = self.get() as f64;
        let window_secs = self.window_size.as_secs_f64();
        if window_secs == 0.0 {
            return 0.0;
        }
        count * (per.as_secs_f64() / window_secs)
    }

    /// Returns the rate of the current bucket (converted to per second).
    #[inline]
    #[allow(dead_code)]
    pub fn recent_bucket_rate_per_second(&self) -> f64 {
        let now = Instant::now();
        let index = self.bucket_index(now);
        let bucket = &self.buckets[index];

        if now.duration_since(bucket.timestamp) <= self.bucket_duration {
            let duration_secs = self.bucket_duration.as_secs_f64();
            if duration_secs > 0.0 {
                bucket.count as f64 / duration_secs
            } else {
                0.0
            }
        } else {
            // Current bucket is expired
            0.0
        }
    }

    /// Returns the count in the current (non-expired) bucket.
    #[inline]
    pub fn recent_get(&self) -> usize {
        let now = Instant::now();
        let index = self.bucket_index(now);
        let bucket = &self.buckets[index];
        if now.duration_since(bucket.timestamp) <= self.bucket_duration {
            bucket.count
        } else {
            0
        }
    }

    /// Computes the index of the bucket for the current time.
    #[inline]
    fn bucket_index(&self, now: Instant) -> usize {
        let elapsed = now.elapsed().as_millis() as u64;
        ((elapsed / self.bucket_duration.as_millis() as u64) % self.bucket_count as u64) as usize
    }
}
