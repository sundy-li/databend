---
title: SHOW SETTINGS
---

Shows the databend's [system settings](../../13-sql-reference/20-system-tables/system-settings.md).

You can change it by set command, like `set max_threads = 1`.

## Syntax

```
SHOW SETTINGS
```

## Examples

```sql
SHOW SETTINGS;
+---------------------------------------+-------------+-------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| name                                  | value       | default     | level   | description                                                                                                                                                                         | type   |
+---------------------------------------+-------------+-------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| collation                             | binary      | binary      | SESSION | Sets the character collation. Available values include "binary" and "utf8".                                                                                                         | String |
| enable_bushy_join                     | 0           | 0           | SESSION | Enables generating a bushy join plan with the optimizer.                                                                                                                            | UInt64 |
| enable_cbo                            | 1           | 1           | SESSION | Enables cost-based optimization.                                                                                                                                                    | UInt64 |
| enable_distributed_eval_index         | 1           | 1           | SESSION | Enables evaluated indexes to be created and maintained across multiple nodes.                                                                                                       | UInt64 |
| enable_query_result_cache             | 0           | 0           | SESSION | Enables caching query results to improve performance for identical queries.                                                                                                         | UInt64 |
| enable_runtime_filter                 | 0           | 0           | SESSION | Enables runtime filter optimization for JOIN.                                                                                                                   | UInt64 |
| flight_client_timeout                 | 60          | 60          | SESSION | Sets the maximum time in seconds that a flight client request can be processed.                                                                                                     | UInt64 |
| group_by_two_level_threshold          | 20000       | 20000       | SESSION | Sets the number of keys in a GROUP BY operation that will trigger a two-level aggregation.                                                                                          | UInt64 |
| hide_options_in_show_create_table     | 1           | 1           | SESSION | Hides table-relevant information, such as SNAPSHOT_LOCATION and STORAGE_FORMAT, at the end of the result of SHOW TABLE CREATE.                                                      | UInt64 |
| input_read_buffer_size                | 1048576     | 1048576     | SESSION | Sets the memory size in bytes allocated to the buffer used by the buffered reader to read data from storage.                                                                        | UInt64 |
| load_file_metadata_expire_hours       | 168         | 168         | SESSION | Sets the hours that the metadata of files you load data from with COPY INTO will expire in.                                                                                         | UInt64 |
| max_block_size                        | 65536       | 65536       | SESSION | Sets the maximum byte size of a single data block that can be read.                                                                                                                 | UInt64 |
| max_execute_time                      | 0           | 0           | SESSION | Sets the maximum query execution time in seconds. Setting it to 0 means no limit.                                                                                                   | UInt64 |
| max_inlist_to_or                      | 3           | 3           | SESSION | Sets the maximum number of values that can be included in an IN expression to be converted to an OR operator.                                                                       | UInt64 |
| max_memory_usage                      | 12911303065 | 12911303065 | SESSION | Sets the maximum memory usage in bytes for processing a single query.                                                                                                               | UInt64 |
| max_result_rows                       | 0           | 0           | SESSION | Sets the maximum number of rows that can be returned in a query result when no specific row count is specified. Setting it to 0 means no limit.                                     | UInt64 |
| max_storage_io_requests               | 48          | 48          | SESSION | Sets the maximum number of concurrent I/O requests.                                                                                                                                 | UInt64 |
| max_threads                           | 16          | 16          | SESSION | Sets the maximum number of threads to execute a request.                                                                                                                            | UInt64 |
| parquet_uncompressed_buffer_size      | 2097152     | 2097152     | SESSION | Sets the byte size of the buffer used for reading Parquet files.                                                                                                                    | UInt64 |
| prefer_broadcast_join                 | 1           | 1           | SESSION | Enables broadcast join.                                                                                                                                                             | UInt64 |
| query_result_cache_allow_inconsistent | 0           | 0           | SESSION | Determines whether Databend will return cached query results that are inconsistent with the underlying data.                                                                        | UInt64 |
| query_result_cache_max_bytes          | 1048576     | 1048576     | SESSION | Sets the maximum byte size of cache for a single query result.                                                                                                                      | UInt64 |
| query_result_cache_ttl_secs           | 300         | 300         | SESSION | Sets the time-to-live (TTL) in seconds for cached query results. Once the TTL for a cached result has expired, the result is considered stale and will not be used for new queries. | UInt64 |
| quoted_ident_case_sensitive           | 1           | 1           | SESSION | Determines whether Databend treats quoted identifiers as case-sensitive.                                                                                                            | UInt64 |
| retention_period                      | 12          | 12          | SESSION | Sets the retention period in hours.                                                                                                                                                 | UInt64 |
| sandbox_tenant                        |             |             | SESSION | Injects a custom 'sandbox_tenant' into this session. This is only for testing purposes and will take effect only when 'internal_enable_sandbox_tenant' is turned on.                | String |
| spilling_bytes_threshold_per_proc     | 0           | 0           | SESSION | Sets the maximum amount of memory in bytes that an aggregator can use before spilling data to storage during query execution.                                                       | UInt64 |
| sql_dialect                           | PostgreSQL  | PostgreSQL  | SESSION | Sets the SQL dialect. Available values include "PostgreSQL", "MySQL", and "Hive".                                                                                                   | String |
| storage_fetch_part_num                | 2           | 2           | SESSION | Sets the number of partitions that are fetched in parallel from storage during query execution.                                                                                     | UInt64 |
| storage_io_max_page_bytes_for_read    | 524288      | 524288      | SESSION | Sets the maximum byte size of data pages that can be read from storage in a single I/O operation.                                                                                   | UInt64 |
| storage_io_min_bytes_for_seek         | 48          | 48          | SESSION | Sets the minimum byte size of data that must be read from storage in a single I/O operation when seeking a new location in the data file.                                           | UInt64 |
| storage_read_buffer_size              | 1048576     | 1048576     | SESSION | Sets the byte size of the buffer used for reading data into memory.                                                                                                                 | UInt64 |
| timezone                              | UTC         | UTC         | SESSION | Sets the timezone.                                                                                                                                                                  | String |
| unquoted_ident_case_sensitive         | 0           | 0           | SESSION | Determines whether Databend treats unquoted identifiers as case-sensitive.                                                                                                          | UInt64 |
+---------------------------------------+-------------+-------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
```
