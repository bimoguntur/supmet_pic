{{ config(schema='data_mart') }}
{{ config(tags=['supermetrics_optimize']) }}
{{
    config(
        partition_by ={
            "field": "date_id",
            "data_type": "date"
        }
    )
}}

with tbl_start_acquisition as (
    select 
    agent_acquisition_name,
    min(start_date_acquisition) as start_date_acquisition
    from `kitabisa-data-team.data_mart.dt_donation_details`
    group by 1
),

tbl_donation as (
    select 
    project_url as parent_url,
    child_short_url,
    --verified,
    project_id as parent_id,
    child_id,
    source_category_level1,
    utm_campaign,
    campaigner_full_name,
    campaigner_organization_status,
    a.agent_acquisition_name,
    b.start_date_acquisition ,
    flag_details,
    a.start_date_acquisition start_date_acquisiton_campaign ,
    --flag_support_details,
    coalesce(child_short_url,project_url) as url_donation,
    coalesce(child_id,project_id) as projects_id,
    cast(verified as date)as verified_day,--to_date(to_char(verified,'YYYY-MM-DD','YYYY-MM-DD')) 
    concat(extract(year from verified),'-', extract(month from verified)) as verified_month,--to_char(verified,'YYYY-MM') as verified_month,
    case 
        when source_category_level1='Ads' and 
        trim(lower(split(utm_campaign,'_')[SAFE_OFFSET(1)]))=trim(lower(coalesce(child_short_url,project_url) )) then  
        utm_campaign
        when source_category_level1='Ads'
        and trim(lower(split(utm_campaign,'_')[SAFE_OFFSET(1)]))<>trim(lower(coalesce(child_short_url,project_url) )) then 'Other Ads'
        end as ad_name_donation,
    case
        when project_url='bisaberobat' then 'Bisa Berobat'
        when project_url='bisasembuhhospital' then 'Bisa Sembuh Hospital'
        when project_url='bisasembuhrscm' then 'RSCM'
        when project_url='bisasembuhrshs' then 'RSHS'
        when campaigner_organization_status = 'PERSONAL' then 'PERSONAL'
        else campaigner_full_name
        end as partner_float_funding,
    case
        when cast(flag_support_details as string) like '%"optimize_by_ads":true%'
        then TRUE
        else FALSE
        end as optimize_by_ads,
    sum(amount) as gdv,
    count(amount) as trx

    from `kitabisa-data-team.data_warehouse.f_donation` a
    left join tbl_start_acquisition b
    on a.agent_acquisition_name=b.agent_acquisition_name
    where lower(acquisition_by) like '%hospital%' 
    and donation_statuses_id in (4,6)
    --and verified > '2021-01-01'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

),purchase as (
    SELECT 
    cast(date as date) as date_purchase, 
    profileid,
    trim((split(ad_name,'_')[SAFE_OFFSET(1)])) short_url, 
    ad_name,
    nullif(sum(offsite_conversions_fb_pixel_purchase),0) offsite_conversions_fb_pixel_purchase,
    nullif(sum(purchase_conversion_value), 0) purchase_conversion_value,
    nullif(sum(landing_page_views), 0) as landing_page_views
    FROM `kitabisa-data-team.data_lake.supermetrics_facebook_ads_website_purchase`
    group by 1,2,3,4
),

fb as (
    SELECT
    date as date_ads,
    fb_ads.ad_name,
    short_url,
    profileid,
    sum(cost) as cost,
    sum(impressions) as impressions,
    sum(action_link_click) as action_link_click,
    sum(fb_ads.landing_page_views) as landing_page_views,
    sum(website_purchase) as website_purchase,
    sum(fb_ads.purchase_conversion_value) as purchase_conversion_value,
    FROM data_warehouse.f_supermetrics_facebook_ads as fb_ads
    group by 1,2,3,4
),
-- Tabel fb left join purchase
fb_purchase as (
    SELECT
    coalesce(date_ads,date_purchase)date_ads,
    coalesce(fb_ads.ad_name,purchase.ad_name) ad_name,
    coalesce(fb_ads.short_url,purchase.short_url) short_url,
    cost as cost,
    impressions as impressions,
    action_link_click as action_link_click,
    coalesce(purchase.landing_page_views, fb_ads.landing_page_views) as landing_page_views,
    coalesce(offsite_conversions_fb_pixel_purchase, website_purchase) as website_purchase,
    coalesce(purchase.purchase_conversion_value, fb_ads.purchase_conversion_value) as purchase_conversion_value,
    from fb fb_ads
    full outer join  purchase
    on fb_ads.profileid = purchase.profileid and fb_ads.ad_name = purchase.ad_name and fb_ads.date_ads = purchase.date_purchase
    
),

tbl_ads as (
        select  
    date_ads as date_ads,
    ad_name_ads,
    short_url as url_ads,
    month_ads,
    ads_source,
    start_date_ads,
    start_date_url,
    first_value(ad_name_ads) over(partition by short_url,month_ads order by month_ads asc, start_date_ads desc) as pic_ad_name,
    first_value(ad_name_ads) over(partition by short_url order by start_date_ads desc) as last_ad_name,
    sum(cost) as cost,
    sum(landing_page_views) as landing_page_views,
    sum(impressions) as impressions,
    sum(action_link_click) as action_link_click,
    sum(website_purchase) as website_purchase,
    sum(purchase_conversion_value) as purchase_conversion_value,
    'TRUE' as optimize,
    from (
        select
        date_ads,
        ad_name as ad_name_ads,
        short_url,
        ads_source, 
        concat(extract(year from date_ads),'-', extract(month from date_ads)) as month_ads,
        min(date_ads) over (partition by left(ad_name,100) order by date_ads asc)as start_date_ads,
        min(date_ads) over (partition by short_url order by date_ads asc) as start_date_url,
        cost,
        landing_page_views,
        impressions,
        website_purchase ,
        action_link_click ,
        purchase_conversion_value,
        from ( 
            Select date_ads,
            ad_name ,
            short_url ,
            cost ,
            coalesce(landing_page_views, 0) as landing_page_views,
            impressions as impressions,
            action_link_click as action_link_click,
            coalesce(website_purchase, 0) as website_purchase,
            coalesce(purchase_conversion_value,0)as purchase_conversion_value,
            'fb' as ads_source
            from fb_purchase
            where (split(ad_name,'_')[SAFE_OFFSET(0)]) = 'HOSPITAL'
            UNION ALL 
            select date,
            ad_name ,
            short_url ,
            cost ,
            0 as landing_page_views,
            0 as impressions,
            0 as action_link_click,
            0 as website_purchase,
            0 as purchase_conversion_value,
            'tiktok' as ads_source
            from `kitabisa-data-team.data_warehouse.f_supermetrics_tiktok_ads`
            where (split(ad_name,'_')[SAFE_OFFSET(0)]) = 'HOSPITAL'
        ) as ads 
    ) 
    --where date > '2021-01-01'
    group by 1,2,3,4,5,6,7

),
tbl_ads_donation as(
    select  
    parent_url,
    child_short_url,
    parent_id,
    child_id,
    --verified,
    Coalesce(source_category_level1,'no donation') as source_category_level1,
    utm_campaign,
    campaigner_full_name,
    partner_float_funding,
    campaigner_organization_status,
    agent_acquisition_name,
    start_date_acquisition ,
    start_date_acquisiton_campaign,
    flag_details,
    --flag_support_details,
    projects_id,
    coalesce (cast(optimize_by_ads as string),optimize) as optimize_by_ads,
    start_date_ads,
    start_date_url,
    pic_ad_name,
    last_ad_name,
    date_ads,
    ads_source,
    Coalesce(ad_name_ads,ad_name_donation) as ads_name,
    Coalesce(verified_month,month_ads) as month_id,
    Coalesce(verified_day,date_ads) as date_id,
    Coalesce(url_donation,url_ads) as url,
    Coalesce(gdv,0) as gdv,
    Coalesce(trx,0) as trx,
    Coalesce(cost,0) as cost,
    coalesce(action_link_click,0) as action_link_click,
    Coalesce(landing_page_views,0) as landing_page_views,
    Coalesce(impressions,0) as impressions,
    Coalesce(website_purchase,0) as website_purchase,
    Coalesce(purchase_conversion_value,0) as purchase_conversion_value


    from tbl_ads  
    full outer join tbl_donation  
    on tbl_ads.ad_name_ads=tbl_donation.ad_name_donation  and tbl_ads.date_ads=tbl_donation.verified_day and trim(lower(tbl_ads.url_ads))=trim(lower(tbl_donation.url_donation)) and cast(tbl_ads.optimize as boolean)=tbl_donation.optimize_by_ads
    --where tbl_donation.verified_day  > '2021-01-01'
),
tbl_ga as (
    select
    concat(url, "_", campaign, "_", date_id) as url_campaign_date,
    sum(page_views) as page_views,
    sum(unique_page_views) as unique_page_views
    from (select * from `kitabisa-data-team.data_warehouse.f_daily_ga` )
    group by 1
),
tbl_ads_donation_ga as (
    select 
    tbl_ads_donation.*,
    coalesce(page_views,0) as ga_page_views,
    coalesce(unique_page_views,0) as ga_unique_page_views
    from tbl_ads_donation left join tbl_ga
    on concat(tbl_ads_donation.url, "_", left(tbl_ads_donation.ads_name,100), "_", tbl_ads_donation.date_id) = tbl_ga.url_campaign_date
),
tbl_ads_donation_proj as (
    select first_value(parent_url) over(partition by url order by gdv desc) as parent_url,
    first_value(parent_id) over(partition by url order by gdv desc) as parent_id,
    first_value(campaigner_full_name) over(partition by url order by gdv desc) as campaigner_full_name,
    first_value(partner_float_funding) over(partition by url order by gdv desc) as partner_float_funding,
    first_value(campaigner_organization_status) over(partition by url order by gdv desc) as campaigner_organization_status,
    first_value(hospital_name) over(partition by url order by gdv desc) as hospital_name,
    Coalesce(Coalesce(pic_ad_name, first_value(pic_ad_name) over(partition by url,month_id order by month_id asc,cost desc)),first_value(last_ad_name) over(partition by url order by cost desc)) as pic_name,
    a.agent_acquisition_name,
    a.start_date_acquisition,
    start_date_acquisiton_campaign,
    flag_details,
    child_id,
    source_category_level1,
    a.utm_campaign,
    child_short_url ,coalesce(a.projects_id,b.project_id) as project_id,
    upper(optimize_by_ads) as optimize_by_ads ,
    --verified,
    start_date_ads,
    start_date_url,
    pic_ad_name,
    last_ad_name,
    date_ads,
    ads_name,
    ads_source,
    month_id,
    date_id,
    url,
    gdv,
    trx,
    cost,
    ga_page_views,
    ga_unique_page_views,
    landing_page_views,
    impressions,
    action_link_click,
    website_purchase,
    purchase_conversion_value,
    expired,
    launched,
    start_date_ads_by_agency,
    final_donation_percentage,
    funding_target 
    from tbl_ads_donation_ga  a 
    left join `kitabisa-data-team.data_mart.dt_project_details` b 
    on trim(lower(a.url)) = trim(lower(b.short_url))
    --where date_id > '2021-01-01'
),

tbl_tam as(
    select distinct 
    url
    ,first_value(kode_rs) over(partition by url order by filter_row desc) kode_rs
    from (
        select *
        , row_number() over (partition by url) as filter_row
        from `kitabisa-data-team.data_lake.gsheet_tam_hospital_acquisition_2019_2020`
    )
    
),
tbl_rs as (
    select 
    url,
    tam.kode_rs,
    rs_name,
    city,
    hosp_list.provinsi,
    hospital_regional,
    rs_type,
    rs_class,
    rs_group,
    organizer,
    concat(rs_name,'_',city,'_',hosp_list.kode_rs) as rs_detail
    from tbl_tam as tam
    left join `kitabisa-data-team.data_mart.dt_hospital_list` as hosp_list
    on tam.kode_rs = cast(hosp_list.kode_rs as string)
    where tam.kode_rs is not null

),
tbl_ads_donation_proj_rs as (
    select 
    tbl_ads_donation_proj.*,
    kode_rs,
    rs_name,
    city,
    provinsi,
    hospital_regional,
    rs_type,
    rs_class,
    rs_group,
    organizer,
    from tbl_ads_donation_proj 
    left join tbl_rs 
    on trim(lower(tbl_ads_donation_proj.url))=trim(lower(tbl_rs.url)) 
),
tbl_funnel as (
    select * 
    from (
        select
        short_url,
        timestamp as ready_date,
        --submit_date,
        grade,
        row_number() over (partition by short_url order by case when timestamp = '' then null  else cast(timestamp as date) end desc) as filter_row
        from `kitabisa-data-team.data_lake.gsheet_funneling_worklist`
        )
    where filter_row  = 1
),
tbl_ads_donation_proj_rs_funnel as(
    select a.*,
    b.short_url,
    b.ready_date,
    --submit_date,
    b.grade,
    from tbl_ads_donation_proj_rs a
    left join  tbl_funnel  b
    on trim(lower(a.url))=trim(lower(b.short_url))
),

previewlink as (
    select distinct * from(
    select distinct ad_name,
    date_id,
    row_number() over (partition by ad_name order by date_id desc) as filter_row,
    preview_link as preview_link
    from `kitabisa-data-team.data_mart.dt_previewlink_ads`
    
    )
    where filter_row=1
)
---- output
select 
tbl_ads_donation_proj_rs_funnel.*,
previewlink.date_id as submit_date,
preview_link
from tbl_ads_donation_proj_rs_funnel 
left join previewlink  
on trim(left(tbl_ads_donation_proj_rs_funnel.ads_name,120))=trim(left(previewlink.ad_name,120))
