{{ config(schema='data_mart') }}
{{ config(tags=['run_all_jobs']) }}
{{
    config(
        partition_by ={
            "field": "month_id",
            "data_type": "date"
        }
    )
}}


select month_id, metrics_name, acqusition_channel ,count(distinct user_id) metrics_number
from data_warehouse.f_donor_user_stage_journey
group by 1,2,3 
