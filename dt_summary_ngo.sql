with 
tbl_donation as (
    select 
    project_url as parent_url,
    child_short_url,
    project_id as parent_id,
    child_id,
    source_category_level1,
    utm_campaign,
    campaigner_id,
    campaigner_full_name,
    campaigner_organization_status,
    start_date_acquisition,
    a.agent_acquisition_name,
    flag_details,
    JSON_EXTRACT_SCALAR(flag_details,'$.agent_name_ads_by_agency') agent_optimize,
    coalesce(child_short_url,project_url) as url_donation,
    coalesce(child_id,project_id) as project_id,
    cast(verified as date)as verified_day, 
    date_trunc(date(verified),month) as verified_month,
    case 
        when source_category_level1='Ads' and 
        trim(lower(split(utm_campaign,'_')[SAFE_OFFSET(1)]))=trim(lower(coalesce(child_short_url,project_url) )) then  
        utm_campaign
        when source_category_level1='Ads'
        and trim(lower(split(utm_campaign,'_')[SAFE_OFFSET(1)]))<>trim(lower(coalesce(child_short_url,project_url) )) then 'Other Ads'
        end as ad_name_donation,
    case
        when cast(flag_support_details as string) like '%"optimize_by_ads":true%'
        then TRUE
        else FALSE
        end as optimize_by_ads,
    sum(amount) as gdv,
    count(amount) as trx

    from `kitabisa-data-team.data_warehouse.f_donation` a
    where acquisition_by in ('small_ngo_acquisition','impacts_acquisition') 
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
    a.short_url as url_ads,
    project_id,
    month_ads,
    ads_source,
    start_date_ads,
    start_date_url,
    first_value(ad_name_ads) over(partition by a.short_url,month_ads order by month_ads asc, start_date_ads desc,sum(cost)desc) as pic_ad_name,
    first_value(ad_name_ads) over(partition by a.short_url order by start_date_ads desc,sum(cost) desc) as last_ad_name,
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
        date_trunc(date_ads,month) month_ads,
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
            where (split(ad_name,'_')[SAFE_OFFSET(0)]) in ('IMPACTS','YAYASAN-MED','YAYASAN-NONMED')
            
            UNION ALL 
            
            select date,
            ad_name ,
            short_url ,
            cost ,
            page_browse landing_page_views,
            impression,
            click,
            0 as website_purchase,
            conversions purchase_conversion_value,
            'tiktok' as ads_source
            from (
                select *,(split(ad_name,'_')[SAFE_OFFSET(0)]) skema
                from `kitabisa-data-team.data_warehouse.f_supermetrics_tiktok_ads`)
            where skema  in ('IMPACTS','YAYASAN-MED','YAYASAN-NONMED') 
            or (lower(ad_name) like '%mp4%' and 
            (lower(skema) not like '%inbound%' and lower(skema) not like '%hospital%' and lower(skema) not like '%program%' and lower(skema) not like '%online%') 
            )
        ) as ads 
    ) a
    left join `kitabisa-data-team.data_warehouse.f_project` b on trim(lower(a.short_url ))=trim(lower(b.short_url))
    --where date > '2021-01-01'
    group by 1,2,3,4,5,6,7,8
),
tbl_ads_donation as(
    select  
    parent_url,
    child_short_url,
    parent_id,
    child_id,
    Coalesce(source_category_level1,'no donation') as source_category_level1,
    utm_campaign,
    campaigner_id,
    campaigner_full_name,
    campaigner_organization_status,
    agent_acquisition_name,
    start_date_acquisition ,
    flag_details,
    coalesce (cast(optimize_by_ads as string),optimize) as optimize_by_ads,
    start_date_ads,
    start_date_url,
    pic_ad_name,
    last_ad_name,
    date_ads,
    agent_optimize,
    ads_source,
    coalesce(tbl_ads.project_id,tbl_donation.project_id) project_id,
    Coalesce(ad_name_ads,ad_name_donation) as ads_name,
    Coalesce(verified_month,month_ads) as month_id,
    Coalesce(verified_day,date_ads) as date_id,
    Coalesce(url_donation,url_ads) as short_url,
    sum(Coalesce(gdv,0)) as gdv,
    sum(Coalesce(trx,0)) as trx,
    sum(Coalesce(cost,0)) as cost,
    sum(coalesce(action_link_click,0)) as action_link_click,
    sum(Coalesce(landing_page_views,0)) as landing_page_views,
    sum(Coalesce(impressions,0)) as impressions,
    sum(Coalesce(website_purchase,0)) as website_purchase,
    sum(Coalesce(purchase_conversion_value,0)) as purchase_conversion_value


    from tbl_ads  
    full outer join tbl_donation  
    on tbl_ads.ad_name_ads=tbl_donation.ad_name_donation  and tbl_ads.date_ads=tbl_donation.verified_day and tbl_ads.project_id=tbl_donation.project_id and cast(tbl_ads.optimize as boolean)=tbl_donation.optimize_by_ads
    --where tbl_donation.verified_day  > '2021-01-01'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,16,17,18,19,20,21,22,23,24,25
),
tbl_pageview as (
    with 
    tbl_clevertap_pageview as (
        select safe_cast(event_timestamp as date) date_id 
        ,campaign_id project_id
        ,lower(trim(b.short_url)) short_url
        ,count(1) page_views 
        ,count(distinct a.user_id) unique_page_views
        from `kitabisa-data-team.data_lake.clevertap_event_campaign_selected` a
        left join `kitabisa-data-team.data_warehouse.f_project` b
        on safe_cast(a.campaign_id as int64)=b.project_id
        group by 1, 2,3
        ),
    tbl_ga_pageviews as (
        select 
        date_id
        ,lower(trim(b.short_url)) short_url
        ,b.project_id
        ,a.campaign utm_campaign
        ,sum(page_views) page_views
        ,sum(unique_page_views) unique_page_views
        from `kitabisa-data-team.data_warehouse.f_daily_ga` a
        left join `kitabisa-data-team.data_warehouse.f_project`  b
        on lower(trim(a.url))=lower(trim(b.short_url))
        group by 1,2,3,4

        union all 

        select 
        date_id
        ,lower(trim(b.short_url)) short_url
        ,b.project_id
        ,a.campaign as url_utm_ga
        ,sum(page_views) page_views
        ,sum(unique_page_views) unique_page_views
        from `kitabisa-data-team.data_warehouse.f_daily_ga`  a
        left join `kitabisa-data-team.data_warehouse.f_project`  b
        on lower(trim(a.url))=safe_cast(b.project_id as string)
        group by 1,2,3,4
        )
    ,tbl_ga_union as (
        select 
        date_id
        ,project_id
        ,utm_campaign 
        ,short_url
        ,page_views
        ,unique_page_views
        ,'Web' as main_source
        from tbl_ga_pageviews 

        union all

        select 
        date_id
        ,safe_cast(project_id as int64) project_id
        ,null as utm_campaign 
        ,short_url
        ,page_views
        ,unique_page_views
        ,'Apps' as main_source
        from tbl_clevertap_pageview   )
    select 
    date_id
    ,project_id
    ,utm_campaign 
    ,short_url
    ,'TRUE' optimize_by_ads
    ,sum(page_views) ga_page_views
    ,sum(unique_page_views) ga_unique_page_views
    from tbl_ga_union 
    where project_id in (
        select distinct project_id
        from tbl_ads_donation
    )
    group by 1,2,3,4
),
tbl_ads_donation_ga as (
    select  
    parent_url,
    child_short_url,
    parent_id,
    child_id,
    Coalesce(source_category_level1,'no donation') as source_category_level1,
    a.utm_campaign,
    campaigner_id,
    campaigner_full_name,
    campaigner_organization_status,
    agent_acquisition_name,
    start_date_acquisition ,
    flag_details,
    a.optimize_by_ads,
    start_date_ads,
    start_date_url,
    pic_ad_name,
    last_ad_name,
    date_ads,
    agent_optimize,
    ads_source,
    ads_name,
    coalesce(a.project_id,b.project_id) project_id,
    Coalesce(month_id,date_trunc(b.date_id,month)) as month_id,
    Coalesce(a.date_id,b.date_id) as date_id,
    Coalesce(a.short_url,b.short_url) as short_url,
    Coalesce(gdv,0) as gdv,
    Coalesce(trx,0) as trx,
    Coalesce(cost,0) as cost,
    coalesce(action_link_click,0) as action_link_click,
    Coalesce(landing_page_views,0) as landing_page_views,
    Coalesce(impressions,0) as impressions,
    Coalesce(website_purchase,0) as website_purchase,
    Coalesce(purchase_conversion_value,0) as purchase_conversion_value,
    coalesce(b.ga_page_views,0) as ga_page_views,
    coalesce(b.ga_unique_page_views,0) as ga_unique_page_views 


    from tbl_ads_donation a
    full outer join  tbl_pageview  b 
    on a.ads_name=b.utm_campaign  and a.date_id=b.date_id and a.project_id=b.project_id  and a.optimize_by_ads=b.optimize_by_ads
),
get_proj as (
    select  
    first_value(parent_url) over(partition by a.short_url order by gdv desc) as parent_url,
    child_short_url,
    first_value(parent_id) over(partition by a.short_url order by gdv desc) as parent_id,
    child_id,
    source_category_level1,
    a.utm_campaign,
    first_value(campaigner_id) over(partition by a.short_url order by gdv desc) as campaigner_id,
    first_value(campaigner_full_name) over(partition by a.short_url order by gdv desc) as campaigner_full_name,
    first_value(campaigner_organization_status) over(partition by a.short_url order by gdv desc) as campaigner_organization_status,
    first_value(a.agent_acquisition_name) over(partition by a.short_url order by gdv desc) as agent_acquisition_name,
    first_value(a.start_date_acquisition) over(partition by a.short_url order by gdv desc) as start_date_acquisition,
    a.flag_details,
    optimize_by_ads,
    start_date_ads,
    start_date_url,
    Coalesce(Coalesce(pic_ad_name, first_value(pic_ad_name) over(partition by a.short_url,month_id order by month_id asc,cost desc)),first_value(last_ad_name) over(partition by a.short_url order by cost desc)) as pic_name,
    pic_ad_name,
    last_ad_name,
    date_ads,
    first_value(agent_optimize) over(partition by a.short_url order by gdv desc) as agent_optimize,
    ads_source,
    ads_name,
    a.project_id,
    month_id,
    date_id,
    a.short_url,
    gdv,
    trx,
    cost,
    action_link_click,
    landing_page_views,
    impressions,
    website_purchase,
    purchase_conversion_value,
    ga_page_views,
    ga_unique_page_views,
    medical, 
    project_categories,
    expired project_expired,
    launched project_launched,
    funding_target,
    b.beneficiary_target,
    b.beneficiary_name,
    cast(JSON_EXTRACT_SCALAR(b.flag_details,'$.start_date_ads_by_agency') as timestamp) start_date_ads_by_agency
    from tbl_ads_donation_ga a
    left join `kitabisa-data-team.data_warehouse.f_project` b on a.project_id=b.project_id
)
,get_parent_ngo as (
    select *
    from (
        select 
        safe_cast(a.user_id as int64) campaigner_id,
        a.nama_cabang_act campaigner_name,
        safe_cast(a.parent_user_id as int64) parent_campaigner_id,
        b.nama_cabang_act as parent_campaigner_name,
        row_number() over(partition by a.user_id) rn
        from `kitabisa-data-team.data_lake.gsheet_ngo_cabang` a
        left join `kitabisa-data-team.data_lake.gsheet_ngo_cabang` b on a.parent_user_id=b.user_id
    )
    where rn=1

),
get_user_status as (
    select
    a.*,
    status_ngo,
    status_yayasan,
    status_whitelist,
    parent_campaigner_id,
    parent_campaigner_name
    from get_proj a
    left join `kitabisa-data-team.data_warehouse.f_users` b on a.campaigner_id=b.user_id
    left join get_parent_ngo c on b.user_id=c.campaigner_id

),
get_region_map as (
        Select a.*,
        provinsi regional_province_name,
        supply regional_supply_name
        from (
        Select
            row_number() over (partition by project_id) as filter_row,
            cast(project_id as string) as project_region_ngo_list,
            regional_id as regional_id_list
        from `kitabisa-data-team.data_lake.mn_paired_offline` 
    ) as a
    left join `kitabisa-data-team.data_lake.gsheet_mapping_region_location`  b on a.regional_id_list=safe_cast(b.region_id as int64)
    where filter_row=1
),
pair_region as (
    select a.*,
    regional_supply_name,
    regional_province_name,
    coalesce(trim(lower(split(pic_name,'_')[SAFE_OFFSET(4)])),agent_optimize) as pic_content,
    trim(lower(split(pic_name,'_')[SAFE_OFFSET(5)])) as pic_visual,
    trim(lower(split(pic_name,'_')[SAFE_OFFSET(6)])) as pic_dm
    from get_user_status  a
    left join get_region_map b on a.project_id=safe_cast(b.project_region_ngo_list   as int64)
),
new_assign_pic as (
    with 
    tbl_pic as (
        select distinct campaign_log_id
        ,pic_supply
        ,a.pic_supply_id
        ,cast(campaign_id as int64) campaign_id
        ,date(a.acted_at) acted_at
        ,tbl_dm.full_name pic_dm
        ,tbl_visual.full_name pic_visual
        ,tbl_content.full_name pic_content
        ,tbl_support.full_name pic_support
        from `kitabisa-data-team.data_warehouse.f_assign_campaign_pic` a
        left join `kitabisa-data-team.data_warehouse.f_users` tbl_content 
        on a.pic_content_id=tbl_content.user_id
        left join `kitabisa-data-team.data_warehouse.f_users` tbl_visual 
        on a.pic_visual_id=tbl_visual.user_id
        left join `kitabisa-data-team.data_warehouse.f_users` tbl_dm
        on a.pic_dm_id=tbl_dm.user_id
        left join `kitabisa-data-team.data_warehouse.f_users` tbl_support 
        on a.pic_support_id=tbl_support.user_id
        left join `kitabisa-data-team.data_warehouse.d_pic_supply` tbl_supply
        on a.pic_supply_id=tbl_supply.pic_supply_id
    ),

    tbl_region as (
        select pic_supply_id,location_id,city,province,
        from `kitabisa-data-team.data_warehouse.d_pic_supply` a
        left join `kitabisa-data-team.data_warehouse.d_region` b
        on a.location_id = b.region_id
    )
    select tbl_pic.*,city,province
    ,concat(coalesce(pic_supply,'-'),'_',coalesce(pic_support,'-'),'_',coalesce(pic_content,'-'),'_',coalesce(pic_visual,'-'),'_',coalesce(pic_dm,'-'),'_',coalesce(province,'-')) pic
    from tbl_pic 
    left join tbl_region 
    on tbl_pic.pic_supply_id=tbl_region.pic_supply_id
),
get_new_pic as (
    SELECT 
    date_id, 
    project_id, 
    ARRAY_AGG(pic ORDER BY case when date_diff(date_id,acted_at,day) < 1 then 1000 else date_diff(date_id,acted_at,day)end  LIMIT 1)[SAFE_OFFSET(0)] AS pic,
    ARRAY_AGG(date_diff(date_id,acted_at,day) ORDER BY case when date_diff(date_id,acted_at,day) < 1 then 1000 else date_diff(date_id,acted_at,day)end  LIMIT 1)[SAFE_OFFSET(0)] AS pic_numb
    FROM pair_region  AS t
    JOIN new_assign_pic  AS r
    ON campaign_id= cast(project_id as int64)
    --where project_id='269164'
    GROUP BY 1,2
    order by 1

),
pairing_pic as (
    select a.* except (pic_content,pic_visual,pic_dm,regional_province_name)
    ,coalesce(case when split(b.pic_x,'_')[SAFE_OFFSET(2)]='-' then null else split(b.pic_x,'_')[SAFE_OFFSET(2)] end,a.pic_content) pic_content
    ,coalesce(case when split(b.pic_x,'_')[SAFE_OFFSET(3)]='-' then null else split(b.pic_x,'_')[SAFE_OFFSET(3)] end,a.pic_visual) pic_visual
    ,coalesce(case when split(b.pic_x,'_')[SAFE_OFFSET(4)]='-' then null else split(b.pic_x,'_')[SAFE_OFFSET(4)] end,a.pic_dm) pic_dm
    ,coalesce(case when split(b.pic_x,'_')[SAFE_OFFSET(5)]='-' then null else split(b.pic_x,'_')[SAFE_OFFSET(5)] end,a.regional_province_name) regional_province_name
    ,coalesce(case when split(b.pic_x,'_')[SAFE_OFFSET(0)]='-' then null else split(b.pic_x,'_')[SAFE_OFFSET(0)] end,a.regional_supply_name) pic_supply
    
    from pair_region  a
    left join (
        select *
        ,case when pic_numb < 1 then null else pic end pic_x
        from get_new_pic        
    ) b 
    on a.project_id=b.project_id and a.date_id=b.date_id 
),
tbl_funnel_grade as (
    select * 
    from (
        select
        safe_cast(project_id as int64) project_id,
        timestamp as ready_date,
        --submit_date,
        grade,
        row_number() over (partition by project_id order by case when timestamp = '' then null  else cast(timestamp as date) end desc) as filter_row
        from `kitabisa-data-team.data_lake.gsheet_funneling_worklist`
        )
    where filter_row  = 1
),
tbl_clickup_grade as (
    select *
    from (
        select 
        safe_cast(trim(split(link_aurum,'/')[SAFE_OFFSET(4)]) as int64) project_id,
        grade,
        row_number() over (partition by link_aurum order by created desc ) rn
        from `kitabisa-data-team.data_warehouse.d_clickup_ngo_squad_barat_tasks`
    )
    where rn=1

    union all 

    select *
    from (
        select 
        safe_cast(trim(split(link_aurum,'/')[SAFE_OFFSET(4)]) as int64) project_id,
        grade,
        row_number() over (partition by link_aurum order by created desc ) rn
        from `kitabisa-data-team.data_warehouse.d_clickup_ngo_squad_timur_tasks`
    )
    where rn=1
),
tbl_previewlink as (
    select distinct * from (
        Select
            ad_name as ad_name,
            preview_link as preview_link,
            date_id,
            row_number() over(partition by ad_name order by date_id DESC) as rn
            from `kitabisa-data-team.data_mart.dt_previewlink_ads`
        )
    where rn = 1

),
pairing_grade as (
    select 
    a.* except (optimize_by_ads), 
    coalesce(b.grade,c.grade) grade,
    preview_link,
    safe_cast(lower(optimize_by_ads) as bool) optimize_by_ads
    from pairing_pic a
    left join tbl_funnel_grade b on a.project_id=b.project_id
    left join tbl_clickup_grade c on a.project_id=c.project_id
    left join tbl_previewlink d on  trim(left(a.ads_name,120))=trim(left(d.ad_name,120))
),
get_final as (
    select 
    date_id,
    month_id,
    project_id,
    short_url,
    parent_url,
    child_short_url,
    parent_id,
    child_id,
    source_category_level1,
    utm_campaign,
    campaigner_id,
    campaigner_full_name,
    campaigner_organization_status,
    parent_campaigner_id,
    parent_campaigner_name,
    status_ngo,
    status_yayasan,
    status_whitelist,
    medical,
    project_categories,project_expired,
    project_launched,
    funding_target,
    beneficiary_name,
    beneficiary_target,
    agent_acquisition_name,
    start_date_acquisition,
    flag_details,
    start_date_ads,
    start_date_url,
    pic_name,
    pic_content,
    pic_visual,
    pic_dm,
    pic_supply,
    regional_province_name,
    agent_optimize,
    ads_source,
    grade,
    preview_link,
    gdv,
    trx,
    cost,
    action_link_click,
    landing_page_views,
    impressions,
    website_purchase,
    purchase_conversion_value,
    ga_page_views,
    ga_unique_page_views,
    start_date_ads_by_agency,
    from pairing_grade 
)
/*
select * -- month_id   ,sum(gdv) gdv,sum(cost) cost 
from get_final       
--where project_id =269164
--and utm_campaigns='YAYASAN-MED_bantuhusnahidrosefalus_maintain-A-vidiokisah_WCVIDEO_TATA_DEVINA_ICA_SUSAN_Cancer Support Kuningan_269164'
--and date_id='2021-07-09'
--group by 1
--order by 1 desc
*/
/*
select distinct date_id,project_id,short_url, pic_content,pic_supply,pic_dm,pic_visual
from pairing_pic 
where project_id=269164
order by date_id
*/


--select project_id,pic_dm, sum(a),sum(comp),sum(a)-sum(comp)
--from(
select  a.pic_dm,sum(gdv) a,0 comp
from pairing_grade a
where date_trunc(date_id,month)='2021-12-01'
and lower(a.pic_dm) in ('kevin','ica')
--and short_url='renovasinurulaklam'
and optimize_by_ads=true 
group by  1

/*
union all 

    select cast(project_id as int64),pic_visual, 0,sum(gdv)
from `kitabisa-data-team.data_mart.dt_supermetrics_pic`
where date_trunc(date_id,month)='2021-12-01'
and lower(pic_visual) in ('rafi','astrid')
--and optimize_by_ads='TRUE'
group by 1,2
) 
group by 1,2
*/
/*
select distinct  month_id,pic_visual,pic_name,pic_ad_name,start_date_ads,first_value(pic_ad_name) over(partition by a.short_url,month_id order by month_id asc,cost desc) bymonth,first_value(last_ad_name) over(partition by a.short_url order by cost desc) last ,last_ad_name
from pairing_grade a
where project_id=225535
--and month_id='2021-12-01'
order by 1
*/
/*
select distinct  month_ads,date_ads, ad_name_ads, pic_ad_name,last_ad_name,start_date_ads
from tbl_ads
where project_id=225535
order by 2 */
