select month_id, metrics_name, acqusition_channel ,count(distinct user_id) metrics_number
from data_warehouse.f_donor_user_stage_journey
group by 1,2,3
