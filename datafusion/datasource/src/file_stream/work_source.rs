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

use std::collections::VecDeque;
use std::sync::Arc;

use crate::PartitionedFile;
use parking_lot::Mutex;

/// Source of unopened files for one `ScanState`.
///
/// Streams that may share work across siblings use [`WorkSource::Shared`],
/// while streams that must preserve their own file order or output partition
/// boundaries keep their files in [`WorkSource::Local`].
pub(super) enum WorkSource {
    /// Files this stream will plan locally without sharing them.
    Local(VecDeque<PartitionedFile>),
    /// Files shared with sibling streams.
    Shared(SharedWorkSource),
}

impl WorkSource {
    /// Pop the next file to plan from this work source.
    pub(super) fn pop_front(&mut self) -> Option<PartitionedFile> {
        match self {
            Self::Local(files) => files.pop_front(),
            Self::Shared(shared) => shared.pop_front(),
        }
    }

    /// Return the number of files that are still waiting to be planned.
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Local(files) => files.len(),
            Self::Shared(shared) => shared.len(),
        }
    }
}

/// Shared source of unopened files that sibling `FileStream`s may steal from.
///
/// Each sibling contributes its initial file group into the shared queue during
/// construction. Later, whichever stream becomes idle first may take the next
/// unopened file from the front of that queue.
#[derive(Debug, Clone)]
pub(crate) struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
    files: Mutex<VecDeque<PartitionedFile>>,
}

impl SharedWorkSource {
    /// Create a shared work source containing the provided unopened files.
    pub(crate) fn new(files: impl IntoIterator<Item = PartitionedFile>) -> Self {
        let files = files.into_iter().collect();
        Self {
            inner: Arc::new(SharedWorkSourceInner {
                files: Mutex::new(files),
            }),
        }
    }

    /// Pop the next file from the shared work queue.
    fn pop_front(&self) -> Option<PartitionedFile> {
        self.inner.files.lock().pop_front()
    }

    /// Return the number of files still waiting in the shared queue.
    fn len(&self) -> usize {
        self.inner.files.lock().len()
    }
}
