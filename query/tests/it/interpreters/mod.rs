// Copyright 2021 Datafuse Labs.
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

mod interpreter_call;
mod interpreter_database_create;
mod interpreter_database_drop;
mod interpreter_database_show_create;
mod interpreter_explain;
mod interpreter_factory_interceptor;
mod interpreter_insert;
mod interpreter_privilege_grant;
mod interpreter_privilege_revoke;
mod interpreter_role_grant;
mod interpreter_role_revoke;
mod interpreter_select;
mod interpreter_setting;
mod interpreter_show_databases;
mod interpreter_show_engines;
mod interpreter_show_functions;
mod interpreter_show_grant;
mod interpreter_show_metrics;
mod interpreter_show_processlist;
mod interpreter_show_roles;
mod interpreter_show_settings;
mod interpreter_show_tab_stat;
mod interpreter_show_tables;
mod interpreter_show_users;
mod interpreter_table_create;
mod interpreter_table_describe;
mod interpreter_table_drop;
mod interpreter_table_rename;
mod interpreter_table_show_create;
mod interpreter_table_truncate;
mod interpreter_use_database;
mod interpreter_user_alter;
mod interpreter_user_create;
mod interpreter_user_drop;
mod interpreter_user_stage;
mod interpreter_user_udf_alter;
mod interpreter_user_udf_create;
mod interpreter_user_udf_drop;
mod plan_schedulers;
