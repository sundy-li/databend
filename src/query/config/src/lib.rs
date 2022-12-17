// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(no_sanitize)]

/// Config mods provide config support.
///
/// We are providing two config types:
///
/// - [`inner::Config`] which will be exposed as [`crate::Config`] will be used in all business logic.
/// - [`outer_v0::Config`] is the outer config for [`inner::Config`] which will be exposed to end-users.
/// - [`global::GlobalConfig`] is a global config singleton of [`crate::Config`].
///
/// It's safe to refactor [`inner::Config`] in anyway, as long as it satisfied the following traits
///
/// - `TryInto<inner::Config> for outer_v0::Config`
/// - `From<inner::Config> for outer_v0::Config`
mod global;
mod inner;
mod outer_v0;
mod version;

pub use global::GlobalConfig;
pub use inner::CatalogConfig;
pub use inner::CatalogHiveConfig;
pub use inner::Config;
pub use inner::QueryConfig;
pub use inner::ThriftProtocol;
pub use outer_v0::StorageConfig;
pub use version::DATABEND_COMMIT_VERSION;
pub use version::QUERY_SEMVER;
