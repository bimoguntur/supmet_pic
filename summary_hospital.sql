with tbl_donation as (
        select 
    project_url as parent_url,
    child_short_url,
    project_id as parent_id,
    child_id,
    source_category_level1,
    utm_campaign,
    campaigner_full_name,
    campaigner_organization_status,
    agent_acquisition_name,
    flag_details,
    --flag_support_details, ini buat apa ya
    coalesce(child_short_url,project_url) as url_donation,
    coalesce(child_id,project_id) as projects_id,
    cast(verified as date)as verified_day,--to_date(to_char(verified,'YYYY-MM-DD','YYYY-MM-DD')) 
    concat(extract(year from verified),'-', extract(month from verified)) as verified_month,--to_char(verified,'YYYY-MM') as verified_month,
    case 
        when source_category_level1='Ads' and 
        trim(lower(split(utm_campaign,'_')[SAFE_OFFSET(1)]))=trim(lower(coalesce(child_short_url,project_url) )) then  
        left(utm_campaign,100)
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

    from `kitabisa-data-team.data_warehouse.f_donation`
    where lower(acquisition_by) like '%hospital%' 
    --and verified > '2021-01-01'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

),

tbl_ads as (
        select  
    date as date_ads,
    ad_name_ads,
    short_url as url_ads,
    month_ads,
    start_date_ads,
    start_date_url,
    first_value(ad_name_ads) over(partition by short_url,month_ads order by month_ads asc, start_date_ads desc) as pic_ad_name,
    first_value(ad_name_ads) over(partition by short_url order by start_date_ads desc) as last_ad_name,
    sum(cost) as cost,
    sum(landing_page_views) as landing_page_views,
    sum(impressions) as impressions,
    sum(website_purchase) as website_purchase,
    sum(purchase_conversion_value) as purchase_conversion_value,
    'TRUE' as optimize,
    from (
        select
        date,
        ad_name as ad_name_ads,
        short_url,
        concat(extract(year from date),'-', extract(month from date)) as month_ads,
        min(date) over (partition by ad_name  order by date asc) as start_date_ads,
        min(date) over (partition by short_url order by date asc) as start_date_url,
        cost,
        landing_page_views,
        impressions,
        website_purchase ,
        purchase_conversion_value,
        from ( 
            Select date,
            ad_name ,
            short_url ,
            cost ,
            landing_page_views,
            impressions,
            action_link_click,
            website_purchase,
            purchase_conversion_value,
            'fb' as ads_source
            from `kitabisa-data-team.data_warehouse.f_supermetrics_facebook_ads`
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
            'tiktok' as ads_source_fb
            from `kitabisa-data-team.data_warehouse.f_supermetrics_tiktok_ads`
            where (split(ad_name,'_')[SAFE_OFFSET(0)]) = 'HOSPITAL'
        ) as ads 
    ) 
    --where date > '2021-01-01'
    group by 1,2,3,4,5,6

),
tbl_ads_donation as(
    select  
    parent_url,
    child_short_url,
    parent_id,
    child_id,
    --verified,
    --amount,
    source_category_level1,
    utm_campaign,
    campaigner_full_name,
    campaigner_organization_status,
    agent_acquisition_name,
    flag_details,
    --flag_support_details,
    projects_id,
    optimize_by_ads,
    start_date_ads,
    start_date_url,
    pic_ad_name,
    last_ad_name,
    --verified_day,
    date_ads,
    Coalesce(ad_name_donation,ad_name_ads) as ads_name,
    Coalesce(verified_month,month_ads) as month_id,
    Coalesce(verified_day,date_ads) as date_id,
    Coalesce(url_donation,url_ads) as url,
    Coalesce(gdv,0) as gdv,
    Coalesce(trx,0) as trx,
    Coalesce(cost,0) as cost,
    Coalesce(landing_page_views,0) as landing_page_views,
    Coalesce(impressions,0) as impressions,
    Coalesce(website_purchase,0) as website_purchase,
    Coalesce(purchase_conversion_value,0) as purchase_conversion_value


    from tbl_ads  
    full outer join tbl_donation  
    on tbl_ads.ad_name_ads=tbl_donation.ad_name_donation  and tbl_ads.date_ads=tbl_donation.verified_day and trim(lower(tbl_ads.url_ads))=trim(lower(tbl_donation.url_donation)) and cast(tbl_ads.optimize as boolean)=tbl_donation.optimize_by_ads
    --where tbl_donation.verified_day  > '2021-01-01'
),
tbl_ads_donation_proj as (
    select tbl_ads_donation.*,
    expired,
    launched,
    start_date_ads_by_agency,
    final_donation_percentage,
    funding_target 
    from tbl_ads_donation 
    left join `kitabisa-data-team.data_mart.dt_project_details` as tbl_project
    on trim(lower(tbl_ads_donation.url)) = trim(lower(tbl_project.short_url))
    --where date_id > '2021-01-01'
),
tbl_rs as (
    select 
    url,
    tam.kode_rs,
    rs_name,
    city,
    provinsi,
    --hospital_region,
    rs_type,
    rs_class,
    --rs_group,
    organizer,
    concat(rs_name,'_',city,'_',hosp_list.kode_rs) as rs_detail
    from `kitabisa-data-team.data_lake.gsheet_tam_hospital_acquisition_2019_2020` as tam
    left join `kitabisa-data-team.data_mart.dt_hospital_list` as hosp_list
    on tam.kode_rs = cast(hosp_list.kode_rs as string)
    where tam.kode_rs is not null

),
tbl_ads_donation_proj_rs as (
    select *
    from tbl_ads_donation_proj 
    left join tbl_rs 
    on trim(lower(tbl_ads_donation_proj.url))=trim(lower(tbl_rs.url)) 
)
,coba as (
select tbl_ads_donation_proj_rs.*,
previewlink.date_id as submit_date,
preview_link
from tbl_ads_donation_proj_rs 
left join (
    select distinct ad_name,
    first_value(date_id) over(partition by ad_name order by date_id desc) as date_id,
    first_value(preview_link) over(partition by ad_name order by date_id desc) as preview_link
    from `kitabisa-data-team.data_mart.dt_previewlink_ads`
    ) as previewlink
on tbl_ads_donation_proj_rs.ads_name=previewlink.ad_name
)
select sum(gdv  ) --date_id, ads_name, url,cost,count(cost)
from tbl_ads_donation 
