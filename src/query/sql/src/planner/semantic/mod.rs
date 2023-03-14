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

mod aggregate_rewriter;
mod distinct_to_groupby;
mod grouping_check;
mod lowering;
mod name_resolution;
mod type_check;

pub use aggregate_rewriter::AggregateRewriter;
pub use distinct_to_groupby::DistinctToGroupBy;
pub use grouping_check::GroupingChecker;
pub use name_resolution::compare_table_name;
pub use name_resolution::normalize_identifier;
pub use name_resolution::IdentifierNormalizer;
pub use name_resolution::NameResolutionContext;
pub use type_check::resolve_type_name;
pub use type_check::resolve_type_name_by_str;
pub use type_check::validate_function_arg;
pub use type_check::TypeChecker;
