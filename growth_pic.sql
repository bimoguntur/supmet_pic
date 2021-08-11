with union_ads as (
    SELECT
        date as date_ads,
        ad_name,
        short_url as short_url_ads,
        concat(cast(EXTRACT(YEAR FROM date) as string),'-',cast(EXTRACT(MONTH FROM date) as string)) as month,
        min(date) over (partition by left(ad_name,100) order by date asc) as start_date_ads,
        min(date) over (partition by short_url order by date asc) as start_date_url,
        cost as cost,
        'fb' as ads_source,
        case 
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(4)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") then null
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%facebook%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(5)]
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%facebook%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(4)]
            else null
        end as pic_content,
        case 
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(6)]), r"[^a-zA-Z ]") then null
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%facebook%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(6)]
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%facebook%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(5)]
            else null
        end as pic_visual,
        case 
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(6)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(7)]), r"[^a-zA-Z ]") then null
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%facebook%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(7)]
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%facebook%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(6)]
            else null
        end as pic_dm
    FROM data_warehouse.f_supermetrics_facebook_ads
    where date >= '2020-01-01'
    UNION ALL
    SELECT
        date as date_ads,
        ad_name,
        short_url as short_url_ads,
        concat(cast(EXTRACT(YEAR FROM date) as string),'-',cast(EXTRACT(MONTH FROM date) as string)) as month,
        min(date) over (partition by left(ad_name,100) order by date asc) as start_date_ads,
        min(date) over (partition by short_url order by date asc) as start_date_url,
        cost as cost,
        'tiktok' as ads_source,
        case 
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(4)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") then null
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%facebook%"  
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(5)]
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%facebook%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(4)]
            else null
        end as pic_content,
        case 
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(6)]), r"[^a-zA-Z ]") then null
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%facebook%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) = "" 
            then split(ad_name, "_")[SAFE_OFFSET(6)]
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%facebook%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(5)]
            else null
        end as pic_visual,
        case 
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(6)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(7)]), r"[^a-zA-Z ]") then null
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) like "%facebook%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(4)])) = "" 
            then split(ad_name, "_")[SAFE_OFFSET(7)]
            when trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%wc%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%ig%" 
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%insta%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%tiktok%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%fb%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) like "%facebook%"
                or  trim(lower(split(ad_name, "_")[SAFE_OFFSET(3)])) = ""
            then split(ad_name, "_")[SAFE_OFFSET(6)]
            else null
        end as pic_dm
    From `kitabisa-data-team.data_warehouse.f_supermetrics_tiktok_ads`
    where date >= '2020-01-01'
),
ads as (
    select 
        date_ads as date_ads,
        ad_name,
        start_date_url,
        short_url_ads as short_url_ads,
        month as month_ads,
        start_date_ads,
        ads_source,
        pic_content,
        pic_visual,
        pic_dm,
        first_value(pic_content IGNORE NULLS) over(partition by short_url_ads,month order by month asc, start_date_ads desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pic_content_log,
        first_value(pic_content IGNORE NULLS) over(partition by short_url_ads order by start_date_ads desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pic_content_lifetime,
        first_value(pic_visual IGNORE NULLS) over(partition by short_url_ads,month order by month asc, start_date_ads desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pic_visual_log,
        first_value(pic_visual IGNORE NULLS) over(partition by short_url_ads order by start_date_ads desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pic_visual_lifetime,
        first_value(pic_dm IGNORE NULLS) over(partition by short_url_ads,month order by month asc, start_date_ads desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pic_dm_log,
        first_value(pic_dm IGNORE NULLS) over(partition by short_url_ads order by start_date_ads desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pic_dm_lifetime,
        sum(cost) as cost
        from union_ads
    group by 1,2,3,4,5,6,7,8,9,10

),
donation as (
    SELECT
        sum(amount) as gdv,
        count(amount) as trx,
        case
            when lower(utm_source_group) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))=lower(coalesce(child_short_url, project_url)) then utm_campaign
            when lower(utm_source_group) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))<>lower(coalesce(child_short_url, project_url)) then 'Other Ads'
            end as utm_ad_name,
        project_url as parent_url,
        project_id,
        acquisition_by as acquisition_by,
        agent_acquisition_name as agent_acquisition_name,   
        JSON_EXTRACT_SCALAR(flag_details,'$.agent_name_ads_by_agency') agent_optimize,
        JSON_EXTRACT_SCALAR(flag_details,'$.status_activated') status_activated,
        JSON_EXTRACT_SCALAR(flag_support_details,'$.optimize_by_ads') status_optimize,
        coalesce(child_short_url, project_url) as url_donation,
        date(verified) as verified_day, 
        cast(concat(extract(YEAR from verified),'-',extract(MONTH from verified)) as string) as verified_month
        from (select *, split(utm_campaign, '_') utm_campaign_1 from data_warehouse.f_donation) a
    where donation_statuses IN ( 'VERIFIED' , 'PAID')
    and verified >= '2020-01-01'
    group by 3,4,5,6,7,8,9,10,11,12,13
),
ads_donation as (
    Select
        agent_optimize as agent_optimize,
        parent_url as parent_url,
        project_id,
        acquisition_by as acquisition_by,
        agent_acquisition_name as agent_acquisition_name,
        pic_content_log,
        pic_content_lifetime,
        pic_visual_log,
        pic_visual_lifetime,
        pic_dm_log,
        pic_dm_lifetime,
        start_date_url,
        ads_source,
        ad_name,
        utm_ad_name,
        status_optimize,
        status_activated,
        Coalesce(ad_name,utm_ad_name) as ads_name,
        Coalesce(verified_month,month_ads) as month_id,
        Coalesce(verified_day,date_ads) as date_id,
        Coalesce(url_donation,short_url_ads) as url_campaign,
        Coalesce(gdv,0) as gdv,
        Coalesce(cost,0) as cost,
        Coalesce(trx,0) as trx,
    from ads full outer join donation
    on ads.ad_name = donation.utm_ad_name 
        and ads.date_ads = donation.verified_day
        and ads.short_url_ads = donation.url_donation
),
tbl_paired_proj as (
    select *
    from (
        select *,
        row_number() over (partition by project_id order by id desc) as filter_row from data_lake.mn_paired_offline
    )
    where filter_row=1
),
tbl_suppy_proj as (
    select *
    from(
        select *,
        row_number() over (partition by region_id order by nama_region desc) as filter_row from data_lake.gsheet_mapping_region_location
    )
    where filter_row= 1
),
project as (
    Select
        a.project_id as project_id,
        short_url as url_proj,
        supply as regional_supply_name,
        acquisition_by
    from `kitabisa-data-team.data_warehouse.f_project` a
    left join tbl_paired_proj  c on a.project_id = c.project_id
    left join tbl_suppy_proj  d on  CAST(c.regional_id AS STRING) = d.region_id

),
activation as (
    SELECT 
    activated_agent_name,
    campaign_id,
    FROM (
        select 
        row_number() over(partition by campaign_id order by date_id desc) as row,
        split(is_activated_agent_name, chr(34))[SAFE_OFFSET(1)] as activated_agent_name,
        campaign_id
        from `kitabisa-data-team.data_warehouse.d_donation_non_partnership`
        where date_id >= "2020-01-01"
        and campaign_id is not null
    ) where row = 1
),
project_act as (
    select
    project_id,
    url_proj,
    acquisition_by,
    regional_supply_name,
    activated_agent_name,
    from project left join activation
    on project.project_id = cast(campaign_id as int64)  
),
ads_donation_project as (
    select
        Coalesce(first_value(ads_donation.acquisition_by) over(partition by url_campaign order by gdv desc), project_act.acquisition_by) as acquisition_by,
        Coalesce(Coalesce(pic_content_log, first_value(pic_content_log) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(pic_content_lifetime) over(partition by url_campaign order by cost desc)) as pic_content_name,
        Coalesce(Coalesce(pic_visual_log, first_value(pic_visual_log) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(pic_visual_lifetime) over(partition by url_campaign order by cost desc)) as pic_visual,
        Coalesce(Coalesce(pic_dm_log, first_value(pic_dm_log) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(pic_dm_lifetime) over(partition by url_campaign order by cost desc)) as pic_dm,
        ads_name,
        gdv,
        date_id,
        month_id,
        url_campaign,
        agent_optimize,
        status_optimize,
        agent_acquisition_name,
        activated_agent_name,
        regional_supply_name,
        status_activated
    from ads_donation left join project_act on ads_donation.project_id = project_act.project_id
),
growth_team as (
    select 
        date_id,
        month_id,
        url_campaign,
            case when acquisition_by like '%hospital%' then 'Hospital'
                when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') THEN 'NGO'
                when acquisition_by like '%non-partnership%' AND status_activated<>"never" AND status_optimize="true" then 'In-Bound'
            end
            
        as squad,
        case
            when acquisition_by like '%hospital%' then trim(lower(agent_acquisition_name))
            when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') THEN trim(lower(regional_supply_name))
            when acquisition_by like '%non-partnership%' AND status_activated<>"never" AND status_optimize="true" then trim(lower(activated_agent_name))
        end as pic_supply,
        coalesce(trim(lower(pic_content_name)),agent_optimize) as pic_content,
        trim(lower(pic_visual)) as pic_visual,
        trim(lower(pic_dm)) as pic_dm,
        status_optimize,
        gdv    
    from ads_donation_project
),
supply as(
    select 
    trim(lower(key)) as key,
    full_name,
    employee_id,
    manager
    from `kitabisa-data-team.data_lake.gsheet_growth_supply`
),
growth_supply_id as (
    select 
    date_id,
    month_id,
    url_campaign,
    squad,
    manager as lead,
    Coalesce(full_name, pic_supply) as pic_supply_name,
    employee_id as pic_supply_id,
    pic_content,
    pic_visual,
    pic_dm,
    status_optimize,
    gdv
    from growth_team left join supply
    on trim(lower(pic_supply)) = trim(lower(key))
),
content as(
    select 
    trim(lower(key)) as key,
    full_name,
    employee_id as pic_content_id,
    manager,
    from `kitabisa-data-team.data_lake.gsheet_growth_content`
    where full_name not like "%Dewi Rachmanita Syiam%"
),
growth_content_id as (
    select 
    date_id,
    month_id,
    url_campaign,
    squad,
    coalesce(lead, manager) as lead,
    pic_supply_name,
    pic_supply_id,
    Coalesce(full_name, pic_content) as pic_content_name,
    pic_content_id,
    pic_visual,
    pic_dm,
    status_optimize,
    gdv
    from growth_supply_id left join content
    on trim(lower(pic_content)) = trim(lower(key))
),
visual as(
    select 
    trim(lower(key)) as key,
    full_name,
    employee_id as pic_visual_id,
    manager
    from `kitabisa-data-team.data_lake.gsheet_growth_visual`
),
growth_visual_id as (
    select 
    date_id,
    month_id,
    url_campaign,
    squad,
    coalesce(lead, manager) as lead,
    pic_supply_name,
    pic_supply_id,
    pic_content_name,
    pic_content_id,
    Coalesce(full_name, pic_visual) as pic_visual_name,
    pic_visual_id,
    pic_dm,
    status_optimize,
    gdv
    from growth_content_id left join visual
    on trim(lower(pic_visual)) = trim(lower(key))
),
dm as(
    select 
    trim(lower(key)) as key,
    full_name,
    employee_id as pic_dm_id,
    manager
    from `kitabisa-data-team.data_lake.gsheet_growth_dm`
)
    select 
    date_id,
    month_id,
    url_campaign,
    squad,
    coalesce(lead, manager) as lead,
    pic_supply_name,
    pic_supply_id,
    pic_content_name,
    pic_content_id,
    pic_visual_name,
    pic_visual_id,
    Coalesce(full_name, pic_dm) as pic_dm_name,
    pic_dm_id,
    status_optimize,
    gdv
    from growth_visual_id left join dm
    on trim(lower(pic_dm)) = trim(lower(key))
    where status_optimize is not null
