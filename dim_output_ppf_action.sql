/* Define keys, tags and materialization */

{{
config(
materialized='incremental', incremental_strategy='delete+insert', unique_key="sk_dim_oupt_ppf_action", tags=["SC_DIM"]
)

}}

/* CTE to fetch data from source object PPFTTRIGG_HIST for P4 instance */
with source as (
select
source_system,
mandt,
name,
class,
objtypeid,
psa_soft_delete_flag,
psa_id,
psa_audit_datetime as source_audit_datetime

from {{ source("sap_erp","ppfttrgg_hist") }}

{% if is_incremental()%)
changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
metadata$action = 'INSERT'
and source_audit_datetime > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
and source_system in ('P4')
qualify 1 row_number() over(
partition by source_system, mandt, name order by "header_timestamp" desc
) 
{% else %}
where
psa_current_flag = true and source_system in ('P4')
{% endif %}

),

/* CTE to create a model from PPFTTRIGG_HIST as per ADS document +/
final as (
select
ifnull(
{{
dbt_utils.generate_surrogate_key([
'src.source_system', 'src.mandt', 'src.name' ])
}}, '-1'
) as sk_dim_output_ppf_action,
src.name as action_profile,
src.psa_soft_delete_flag as delete_flag, 
src.source_system as source_system, 
src.mandt as client, 
src.objtypeid as object_id, 
current_timestamp() as current_audit_datetime,
src.class as class_name,
src.psa_id as stage_id, 
'PPFTTRIGG_HIST' as stage_source_table,
src.source_audit_datetime as source_audit_datetime

from source as src
)

select
sk_dim_output_ppf_action,
delete_flag,
source_system,
client,
action_profile,
class_name,
object_id,
current_audit_datetime,
stage_id,
stage_source_table,
source_audit_datetime

from final


