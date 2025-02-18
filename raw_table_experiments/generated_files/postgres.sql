-- create table --------------------------------
create table "no_raw_tables_experiment"."old_final_table_10mb" ("primary_key" bigint, "cursor" timestamp, "string" varchar, "bool" boolean, "integer" bigint, "float" decimal(38, 9), "date" date, "ts_with_tz" timestamp with time zone, "ts_without_tz" timestamp, "time_with_tz" time with time zone, "time_no_tz" time, "array" jsonb, "json_object" jsonb, "_airbyte_raw_id" varchar(36) not null, "_airbyte_extracted_at" timestamp with time zone not null, "_airbyte_generation_id" bigint, "_airbyte_meta" jsonb not null);
create index on "no_raw_tables_experiment"."old_final_table_10mb"("primary_key", "cursor", "_airbyte_extracted_at");
create index on "no_raw_tables_experiment"."old_final_table_10mb"("_airbyte_extracted_at");
create index on "no_raw_tables_experiment"."old_final_table_10mb"("_airbyte_raw_id");










-- "fast" T+D query -------------------------------
insert into "no_raw_tables_experiment"."old_final_table_10mb" ("primary_key", "cursor", "string", "bool", "integer", "float", "date", "ts_with_tz", "ts_without_tz", "time_with_tz", "time_no_tz", "array", "json_object", "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_generation_id", "_airbyte_meta") with "intermediate_data" as (select cast(case when ("_airbyte_data" -> 'primary_key' is null or JSONB_TYPEOF("_airbyte_data" -> 'primary_key') = 'null') then null else cast("_airbyte_data" -> 'primary_key' as varchar) end as bigint) as "primary_key", cast(case when ("_airbyte_data" -> 'cursor' is null or JSONB_TYPEOF("_airbyte_data" -> 'cursor') = 'null') then null else cast("_airbyte_data" -> 'cursor' as varchar) end as timestamp) as "cursor", "_airbyte_data" -> 'string' #>> '{}' as "string", cast(case when ("_airbyte_data" -> 'bool' is null or JSONB_TYPEOF("_airbyte_data" -> 'bool') = 'null') then null else cast("_airbyte_data" -> 'bool' as varchar) end as boolean) as "bool", cast(case when ("_airbyte_data" -> 'integer' is null or JSONB_TYPEOF("_airbyte_data" -> 'integer') = 'null') then null else cast("_airbyte_data" -> 'integer' as varchar) end as bigint) as "integer", cast(case when ("_airbyte_data" -> 'float' is null or JSONB_TYPEOF("_airbyte_data" -> 'float') = 'null') then null else cast("_airbyte_data" -> 'float' as varchar) end as decimal(38, 9)) as "float", cast(case when ("_airbyte_data" -> 'date' is null or JSONB_TYPEOF("_airbyte_data" -> 'date') = 'null') then null else cast("_airbyte_data" -> 'date' as varchar) end as date) as "date", cast(case when ("_airbyte_data" -> 'ts_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_with_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_with_tz' as varchar) end as timestamp with time zone) as "ts_with_tz", cast(case when ("_airbyte_data" -> 'ts_without_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_without_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_without_tz' as varchar) end as timestamp) as "ts_without_tz", cast(case when ("_airbyte_data" -> 'time_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_with_tz') = 'null') then null else cast("_airbyte_data" -> 'time_with_tz' as varchar) end as time with time zone) as "time_with_tz", cast(case when ("_airbyte_data" -> 'time_no_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_no_tz') = 'null') then null else cast("_airbyte_data" -> 'time_no_tz' as varchar) end as time) as "time_no_tz", cast(case when ("_airbyte_data" -> 'array' is null or JSONB_TYPEOF("_airbyte_data" -> 'array') <> 'array') then null else "_airbyte_data" -> 'array' end as jsonb) as "array", cast(case when ("_airbyte_data" -> 'json_object' is null or JSONB_TYPEOF("_airbyte_data" -> 'json_object') <> 'object') then null else "_airbyte_data" -> 'json_object' end as jsonb) as "json_object", "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_generation_id", JSONB_BUILD_OBJECT('changes', ARRAY_CAT(ARRAY_REMOVE(cast(array[CASE WHEN ("_airbyte_data" -> 'primary_key' is not null and JSONB_TYPEOF("_airbyte_data" -> 'primary_key') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'primary_key' is null or JSONB_TYPEOF("_airbyte_data" -> 'primary_key') = 'null') then null else cast("_airbyte_data" -> 'primary_key' as varchar) end, cast(null as bigint)) is null) THEN JSONB_BUILD_OBJECT('field', 'primary_key', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'cursor' is not null and JSONB_TYPEOF("_airbyte_data" -> 'cursor') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'cursor' is null or JSONB_TYPEOF("_airbyte_data" -> 'cursor') = 'null') then null else cast("_airbyte_data" -> 'cursor' as varchar) end, cast(null as timestamp)) is null) THEN JSONB_BUILD_OBJECT('field', 'cursor', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , cast(null as jsonb), CASE WHEN ("_airbyte_data" -> 'bool' is not null and JSONB_TYPEOF("_airbyte_data" -> 'bool') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'bool' is null or JSONB_TYPEOF("_airbyte_data" -> 'bool') = 'null') then null else cast("_airbyte_data" -> 'bool' as varchar) end, cast(null as boolean)) is null) THEN JSONB_BUILD_OBJECT('field', 'bool', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'integer' is not null and JSONB_TYPEOF("_airbyte_data" -> 'integer') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'integer' is null or JSONB_TYPEOF("_airbyte_data" -> 'integer') = 'null') then null else cast("_airbyte_data" -> 'integer' as varchar) end, cast(null as bigint)) is null) THEN JSONB_BUILD_OBJECT('field', 'integer', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'float' is not null and JSONB_TYPEOF("_airbyte_data" -> 'float') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'float' is null or JSONB_TYPEOF("_airbyte_data" -> 'float') = 'null') then null else cast("_airbyte_data" -> 'float' as varchar) end, cast(null as decimal(38, 9))) is null) THEN JSONB_BUILD_OBJECT('field', 'float', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'date' is not null and JSONB_TYPEOF("_airbyte_data" -> 'date') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'date' is null or JSONB_TYPEOF("_airbyte_data" -> 'date') = 'null') then null else cast("_airbyte_data" -> 'date' as varchar) end, cast(null as date)) is null) THEN JSONB_BUILD_OBJECT('field', 'date', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'ts_with_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'ts_with_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'ts_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_with_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_with_tz' as varchar) end, cast(null as timestamp with time zone)) is null) THEN JSONB_BUILD_OBJECT('field', 'ts_with_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'ts_without_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'ts_without_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'ts_without_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_without_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_without_tz' as varchar) end, cast(null as timestamp)) is null) THEN JSONB_BUILD_OBJECT('field', 'ts_without_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'time_with_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'time_with_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'time_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_with_tz') = 'null') then null else cast("_airbyte_data" -> 'time_with_tz' as varchar) end, cast(null as time with time zone)) is null) THEN JSONB_BUILD_OBJECT('field', 'time_with_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'time_no_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'time_no_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'time_no_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_no_tz') = 'null') then null else cast("_airbyte_data" -> 'time_no_tz' as varchar) end, cast(null as time)) is null) THEN JSONB_BUILD_OBJECT('field', 'time_no_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'array' is not null and JSONB_TYPEOF("_airbyte_data" -> 'array') not in ('array', 'null')) THEN JSONB_BUILD_OBJECT('field', 'array', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'json_object' is not null and JSONB_TYPEOF("_airbyte_data" -> 'json_object') not in ('object', 'null')) THEN JSONB_BUILD_OBJECT('field', 'json_object', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END ] as jsonb[]), null), ARRAY(SELECT jsonb_array_elements_text("_airbyte_meta"#>'{changes}'))::jsonb[]), 'sync_id', "_airbyte_meta"#>'{sync_id}') as "_airbyte_meta" from "no_raw_tables_experiment"."raw_table_10mb" where "_airbyte_loaded_at" is null), "numbered_rows" as (select *, row_number() over (partition by "primary_key" order by "cursor" desc NULLS LAST, "_airbyte_extracted_at" desc) as "row_number" from "intermediate_data") select "primary_key", "cursor", "string", "bool", "integer", "float", "date", "ts_with_tz", "ts_without_tz", "time_with_tz", "time_no_tz", "array", "json_object", "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_generation_id", "_airbyte_meta" from "numbered_rows" where "row_number" = 1;
delete from "no_raw_tables_experiment"."old_final_table_10mb" where "_airbyte_raw_id" in (select "_airbyte_raw_id" from (select "_airbyte_raw_id", row_number() over (partition by "primary_key" order by "cursor" desc NULLS LAST, "_airbyte_extracted_at" desc) as "row_number" from "no_raw_tables_experiment"."old_final_table_10mb") as "airbyte_ids" where "row_number" <> 1);
update "no_raw_tables_experiment"."raw_table_10mb" set "_airbyte_loaded_at" = current_timestamp where "_airbyte_loaded_at" is null;










-- "slow" T+D query -------------------------------
insert into "no_raw_tables_experiment"."old_final_table_10mb" ("primary_key", "cursor", "string", "bool", "integer", "float", "date", "ts_with_tz", "ts_without_tz", "time_with_tz", "time_no_tz", "array", "json_object", "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_generation_id", "_airbyte_meta") with "intermediate_data" as (select "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'primary_key' is null or JSONB_TYPEOF("_airbyte_data" -> 'primary_key') = 'null') then null else cast("_airbyte_data" -> 'primary_key' as varchar) end, cast(null as bigint)) as "primary_key", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'cursor' is null or JSONB_TYPEOF("_airbyte_data" -> 'cursor') = 'null') then null else cast("_airbyte_data" -> 'cursor' as varchar) end, cast(null as timestamp)) as "cursor", "_airbyte_data" -> 'string' #>> '{}' as "string", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'bool' is null or JSONB_TYPEOF("_airbyte_data" -> 'bool') = 'null') then null else cast("_airbyte_data" -> 'bool' as varchar) end, cast(null as boolean)) as "bool", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'integer' is null or JSONB_TYPEOF("_airbyte_data" -> 'integer') = 'null') then null else cast("_airbyte_data" -> 'integer' as varchar) end, cast(null as bigint)) as "integer", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'float' is null or JSONB_TYPEOF("_airbyte_data" -> 'float') = 'null') then null else cast("_airbyte_data" -> 'float' as varchar) end, cast(null as decimal(38, 9))) as "float", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'date' is null or JSONB_TYPEOF("_airbyte_data" -> 'date') = 'null') then null else cast("_airbyte_data" -> 'date' as varchar) end, cast(null as date)) as "date", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'ts_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_with_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_with_tz' as varchar) end, cast(null as timestamp with time zone)) as "ts_with_tz", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'ts_without_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_without_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_without_tz' as varchar) end, cast(null as timestamp)) as "ts_without_tz", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'time_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_with_tz') = 'null') then null else cast("_airbyte_data" -> 'time_with_tz' as varchar) end, cast(null as time with time zone)) as "time_with_tz", "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'time_no_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_no_tz') = 'null') then null else cast("_airbyte_data" -> 'time_no_tz' as varchar) end, cast(null as time)) as "time_no_tz", cast(case when ("_airbyte_data" -> 'array' is null or JSONB_TYPEOF("_airbyte_data" -> 'array') <> 'array') then null else "_airbyte_data" -> 'array' end as jsonb) as "array", cast(case when ("_airbyte_data" -> 'json_object' is null or JSONB_TYPEOF("_airbyte_data" -> 'json_object') <> 'object') then null else "_airbyte_data" -> 'json_object' end as jsonb) as "json_object", "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_generation_id", JSONB_BUILD_OBJECT('changes', ARRAY_CAT(ARRAY_REMOVE(cast(array[CASE WHEN ("_airbyte_data" -> 'primary_key' is not null and JSONB_TYPEOF("_airbyte_data" -> 'primary_key') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'primary_key' is null or JSONB_TYPEOF("_airbyte_data" -> 'primary_key') = 'null') then null else cast("_airbyte_data" -> 'primary_key' as varchar) end, cast(null as bigint)) is null) THEN JSONB_BUILD_OBJECT('field', 'primary_key', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'cursor' is not null and JSONB_TYPEOF("_airbyte_data" -> 'cursor') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'cursor' is null or JSONB_TYPEOF("_airbyte_data" -> 'cursor') = 'null') then null else cast("_airbyte_data" -> 'cursor' as varchar) end, cast(null as timestamp)) is null) THEN JSONB_BUILD_OBJECT('field', 'cursor', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , cast(null as jsonb), CASE WHEN ("_airbyte_data" -> 'bool' is not null and JSONB_TYPEOF("_airbyte_data" -> 'bool') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'bool' is null or JSONB_TYPEOF("_airbyte_data" -> 'bool') = 'null') then null else cast("_airbyte_data" -> 'bool' as varchar) end, cast(null as boolean)) is null) THEN JSONB_BUILD_OBJECT('field', 'bool', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'integer' is not null and JSONB_TYPEOF("_airbyte_data" -> 'integer') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'integer' is null or JSONB_TYPEOF("_airbyte_data" -> 'integer') = 'null') then null else cast("_airbyte_data" -> 'integer' as varchar) end, cast(null as bigint)) is null) THEN JSONB_BUILD_OBJECT('field', 'integer', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'float' is not null and JSONB_TYPEOF("_airbyte_data" -> 'float') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'float' is null or JSONB_TYPEOF("_airbyte_data" -> 'float') = 'null') then null else cast("_airbyte_data" -> 'float' as varchar) end, cast(null as decimal(38, 9))) is null) THEN JSONB_BUILD_OBJECT('field', 'float', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'date' is not null and JSONB_TYPEOF("_airbyte_data" -> 'date') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'date' is null or JSONB_TYPEOF("_airbyte_data" -> 'date') = 'null') then null else cast("_airbyte_data" -> 'date' as varchar) end, cast(null as date)) is null) THEN JSONB_BUILD_OBJECT('field', 'date', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'ts_with_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'ts_with_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'ts_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_with_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_with_tz' as varchar) end, cast(null as timestamp with time zone)) is null) THEN JSONB_BUILD_OBJECT('field', 'ts_with_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'ts_without_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'ts_without_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'ts_without_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'ts_without_tz') = 'null') then null else cast("_airbyte_data" -> 'ts_without_tz' as varchar) end, cast(null as timestamp)) is null) THEN JSONB_BUILD_OBJECT('field', 'ts_without_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'time_with_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'time_with_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'time_with_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_with_tz') = 'null') then null else cast("_airbyte_data" -> 'time_with_tz' as varchar) end, cast(null as time with time zone)) is null) THEN JSONB_BUILD_OBJECT('field', 'time_with_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'time_no_tz' is not null and JSONB_TYPEOF("_airbyte_data" -> 'time_no_tz') <> 'null' and "pg_temp"."airbyte_safe_cast"(case when ("_airbyte_data" -> 'time_no_tz' is null or JSONB_TYPEOF("_airbyte_data" -> 'time_no_tz') = 'null') then null else cast("_airbyte_data" -> 'time_no_tz' as varchar) end, cast(null as time)) is null) THEN JSONB_BUILD_OBJECT('field', 'time_no_tz', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'array' is not null and JSONB_TYPEOF("_airbyte_data" -> 'array') not in ('array', 'null')) THEN JSONB_BUILD_OBJECT('field', 'array', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END , CASE WHEN ("_airbyte_data" -> 'json_object' is not null and JSONB_TYPEOF("_airbyte_data" -> 'json_object') not in ('object', 'null')) THEN JSONB_BUILD_OBJECT('field', 'json_object', 'change', 'NULLED', 'reason', 'DESTINATION_TYPECAST_ERROR') ELSE cast(null as jsonb) END ] as jsonb[]), null), ARRAY(SELECT jsonb_array_elements_text("_airbyte_meta"#>'{changes}'))::jsonb[]), 'sync_id', "_airbyte_meta"#>'{sync_id}') as "_airbyte_meta" from "no_raw_tables_experiment"."raw_table_10mb" where "_airbyte_loaded_at" is null), "numbered_rows" as (select *, row_number() over (partition by "primary_key" order by "cursor" desc NULLS LAST, "_airbyte_extracted_at" desc) as "row_number" from "intermediate_data") select "primary_key", "cursor", "string", "bool", "integer", "float", "date", "ts_with_tz", "ts_without_tz", "time_with_tz", "time_no_tz", "array", "json_object", "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_generation_id", "_airbyte_meta" from "numbered_rows" where "row_number" = 1;
delete from "no_raw_tables_experiment"."old_final_table_10mb" where "_airbyte_raw_id" in (select "_airbyte_raw_id" from (select "_airbyte_raw_id", row_number() over (partition by "primary_key" order by "cursor" desc NULLS LAST, "_airbyte_extracted_at" desc) as "row_number" from "no_raw_tables_experiment"."old_final_table_10mb") as "airbyte_ids" where "row_number" <> 1);
update "no_raw_tables_experiment"."raw_table_10mb" set "_airbyte_loaded_at" = current_timestamp where "_airbyte_loaded_at" is null;
