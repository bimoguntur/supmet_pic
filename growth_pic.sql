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
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(4)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") then null
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
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(4)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") then null
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
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(4)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") then null
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
            when regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(4)]), r"[^a-zA-Z ]") or regexp_contains(trim(split(ad_name, "_")[SAFE_OFFSET(5)]), r"[^a-zA-Z ]") then null
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

),donation as (
    SELECT
        sum(amount) as gdv,
        count(amount) as trx,
        case
            when lower(utm_source_group) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))=lower(coalesce(child_short_url, project_url)) then utm_campaign
            when lower(utm_source_group) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))<>lower(coalesce(child_short_url, project_url)) then 'Other Ads'
            end as utm_ad_name,
        project_url as parent_url,
        campaigner_full_name as campaigner_full_name,
        partners as partners,
        acquisition_by as acquisition_by,
        agent_acquisition_name as agent_acquisition_name,   
        JSON_EXTRACT_SCALAR(flag_details,'$.agent_name_ads_by_agency') agent_optimize,
        JSON_EXTRACT_SCALAR(flag_details,'$.status_activated') status_activated,
        JSON_EXTRACT_SCALAR(flag_details,'$.status_ads_by_agency') status_ads_by_agency,
        coalesce(child_short_url, project_url) as url_donation,
        date(verified) as verified_day, 
        cast(concat(extract(YEAR from verified),'-',extract(MONTH from verified)) as string) as verified_month
        from (select *, split(utm_campaign, '_') utm_campaign_1 from data_warehouse.f_donation) a
    where (donation_statuses = 'VERIFIED' OR donation_statuses = 'PAID')
    and verified >= '2020-01-01'
    group by 3,4,5,6,7,8,9,10,11,12,13,14
),
ads_donation as (
    Select
        agent_optimize as agent_optimize,
        parent_url as parent_url,
        campaigner_full_name,
        partners as partners,
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
project as (
    Select
        cast(project_id as string) as project_id,
        short_url as url_proj,
        acquisition_by,
        launched,
        expired,
        final_donation_percentage,
        regional_supply_name,
        activated_agent_name,
    from `kitabisa-data-team.data_mart.dt_project_details`  
),
tbl_employee as (
    select
    coalesce(first_value(ads_donation.acquisition_by) over(partition by url_campaign order by gdv desc), project.acquisition_by) as acquisition_by,
    Coalesce(Coalesce(pic_content_log, first_value(pic_content_log) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(pic_content_lifetime) over(partition by url_campaign order by cost desc)) as pic_content_name,
    Coalesce(Coalesce(pic_visual_log, first_value(pic_visual_log) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(pic_visual_lifetime) over(partition by url_campaign order by cost desc)) as pic_visual,
    Coalesce(Coalesce(pic_dm_log, first_value(pic_dm_log) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(pic_dm_lifetime) over(partition by url_campaign order by cost desc)) as pic_dm,
    gdv,
    date_id,
    month_id,
    url_campaign,
    campaigner_full_name,
    agent_optimize,
    agent_acquisition_name,
    activated_agent_name,
    regional_supply_name,
    cost
from ads_donation left join project on ads_donation.url_campaign = project.url_proj
)
select 
     url_campaign,
     Coalesce(
         (case when acquisition_by like '%hospital%' then 'Hospital'
             when acquisition_by='program_acquisition' OR (acquisition_by='zakat_acquisition' AND campaigner_full_name like '%Kitabisa%') OR ((campaigner_full_name='Peduli Anak Foundation' OR campaigner_full_name='RQV Indonesia') AND date_id <= '2021-03-31') OR (campaigner_full_name='UNHCR Indonesia' OR campaigner_full_name='Yayasan Bina Mulia Bojonegoro') OR ((campaigner_full_name like '%an Ash-Shalihin%' OR campaigner_full_name='Pondok Sedekah Indonesia' OR campaigner_full_name='Pondok Sedekah Sulsel') AND date_id <= '2021-01-31') THEN 'Program, Zakat, & NGO non-Region'
             when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') THEN 'NGO'
             when acquisition_by like '%non-partnership%' OR ((acquisition_by like '%influencer%' OR acquisition_by like '%partnership_ac%') AND date_id > '2021-01-31') then 'In-Bound & Influencer'
         end),
         (case
             when agent_optimize like '%Nur Rahmah%' OR agent_optimize like '%Nadia Aisha%' OR agent_optimize like '%Dwi Astri%' OR agent_optimize like '%Nabil%' OR agent_optimize like '%Naya%' OR agent_optimize like '%Winny%' OR agent_optimize like '%Christabella%' OR agent_optimize like '%Oktaviani%' THEN 'Program, Zakat, & NGO non-Region'

             when agent_optimize like '%Monica%' OR agent_optimize like '%Lindyra%' OR agent_optimize like '%Dela Destri%' THEN 'In-Bound & Influencer'

             when agent_optimize like '%Gabriel%' OR agent_optimize like '%Zahra Matarani%' OR agent_optimize like '%Adinda%' OR agent_optimize like '%Dwi Ayu%' THEN 'Hospital'

             when agent_optimize like '%Shanara%' OR agent_optimize like '%Andaris%' OR agent_optimize like '%Dewi Marisa%' OR agent_optimize like '%Inta Yunita%' OR agent_optimize like '%Aldila%' OR agent_optimize like '%Davin%' OR agent_optimize like '%Rafianti%' OR agent_optimize like '%Annisa Dwi%' OR agent_optimize like '%Shintia%' OR agent_optimize like '%Alya%' OR agent_optimize like '%Tasha%' OR agent_optimize like '%Fania%' OR agent_optimize like '%Alega%' OR agent_optimize like '%Lutfiah%' OR agent_optimize like '%Tata%' OR agent_optimize like '%Nadira%' OR agent_optimize like '%Emilia%' OR agent_optimize like '%Yuliana%' OR agent_optimize like '%Dimas%' OR agent_optimize like '%Vora%' OR agent_optimize like '%Ghilman%' OR agent_optimize like '%Sadida%' OR agent_optimize like '%Vicky%' OR agent_optimize like '%Fajar%' OR agent_optimize like '%Gita%' OR agent_optimize like '%Unike%' OR agent_optimize like '%Clara%' OR agent_optimize like '%Fira Shabrina%' OR agent_optimize like '%Tazkiya%' OR agent_optimize like '%Medina%' OR agent_optimize like '%Nurullita%' THEN 'NGO'
         end)
     ) as squad,
     case
         when acquisition_by like '%hospital%' then agent_acquisition_name
         when acquisition_by='program_acquisition' OR (acquisition_by='zakat_acquisition' AND campaigner_full_name like '%Kitabisa%') OR ((campaigner_full_name='Peduli Anak Foundation' OR campaigner_full_name='RQV Indonesia') AND date_id <= '2021-03-31') OR (campaigner_full_name='UNHCR Indonesia' OR campaigner_full_name='Yayasan Bina Mulia Bojonegoro') OR ((campaigner_full_name like '%an Ash-Shalihin%' OR campaigner_full_name='Pondok Sedekah Indonesia' OR campaigner_full_name='Pondok Sedekah Sulsel') AND date_id <= '2021-01-31') THEN regional_supply_name
         when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') THEN regional_supply_name
        when acquisition_by like '%non-partnership%' OR ((acquisition_by like '%influencer%' OR acquisition_by like '%partnership_ac%') AND date_id > '2021-01-31') then activated_agent_name
    end as pic_supply,
    coalesce(trim(lower(pic_content_name)),agent_optimize) as pic_content,
    trim(lower(pic_visual)) as pic_visual,
    trim(lower(pic_dm)) as pic_dm,
    gdv    
    from tbl_employee
    where pic_content_name is not null
