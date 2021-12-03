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

-- Tabel purchase
with purchase as (
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
-- Tabel ads
ads_1 as (
    SELECT
        date_ads,
        ad_name,
        short_url as short_url_ads,
        concat(cast(EXTRACT(YEAR FROM date_ads) as string),'-',cast(EXTRACT(MONTH FROM date_ads) as string)) as month, --to_char(date,'YYYY-MM') as month,
        min(date_ads) over (partition by left(ad_name,100) order by date_ads asc) as start_date_ads,
        min(date_ads) over (partition by short_url order by date_ads asc) as start_date_url,
        cost as cost,
        coalesce(landing_page_views, 0) as landing_page_views,
        impressions as impressions,
        action_link_click as action_link_click,
        coalesce(website_purchase, 0) as website_purchase,
        coalesce(purchase_conversion_value,0)as purchase_conversion_value,
        'fb' as ads_source
    FROM fb_purchase
    where date_ads >= '2020-01-01'
    UNION ALL
    SELECT
        date as date_ads,
        ad_name,
        short_url as short_url_ads,
        concat(cast(EXTRACT(YEAR FROM date) as string),'-',cast(EXTRACT(MONTH FROM date) as string)) as month, --to_char(date,'YYYY-MM') as month,
        min(date) over (partition by left(ad_name,100) order by date asc) as start_date_ads,
        min(date) over (partition by short_url order by date asc) as start_date_url,
        cost as cost,
        0 as landing_page_views,
        0 as impressions,
        0 as action_link_click,
        0 as website_purchase,
        0 as purchase_conversion_value,
        'tiktok' as ads_source
    From `kitabisa-data-team.data_warehouse.f_supermetrics_tiktok_ads`
    where date >= '2020-01-01'
),
-- tabel ads_preview_link
ads_preview_link as (
  
    select distinct * from (
        Select
            ad_name as l_ad_name,
            preview_link as l_preview_link,
            date_id,
            row_number() over(partition by ad_name order by date_id DESC) as rn
            from `kitabisa-data-team.data_mart.dt_previewlink_ads`
        )
    where rn = 1
),
--- tbl ads_permalink, ads_1 + ads_preview_link
ads_2 as (
    select 
        date_ads as date_ads,
        ad_name,
        --l_preview_link as preview_link,
        start_date_url,
        short_url_ads as short_url_ads,
        month as month_ads,
        --date_id as feeding_date_ads,
        start_date_ads,
        ads_source,
        first_value(ad_name) over(partition by short_url_ads,month order by month asc, start_date_ads desc) as pic_ad_name,
        first_value(ad_name) over(partition by short_url_ads order by start_date_ads desc) as last_ad_name,
        sum(cost) as cost,
        sum(landing_page_views) as landing_page_views,
        sum(impressions) as impressions,
        sum(action_link_click) as action_link_click,
        sum(website_purchase) as website_purchase,
        sum(purchase_conversion_value) as purchase_conversion_value
    from ads_1 --left join ads_preview_link 
    --on ads_1.ad_name = ads_preview_link.l_ad_name
    group by 1,2,3,4,5,6,7
),
donation_1 as (
    SELECT
        sum(amount) as gdv,
        count(amount) as trx,
        sum(case when main_source='Web' then amount end) gdv_pwa,
        sum(case when main_source='Apps' then amount end) gdv_apps,
        case when lower(source_category_level1) like '%ads%'  and main_source<>'3rd Party' then 'Ads'
            when lower(utm_source_group) like '%newsletter%'  and main_source<>'3rd Party' then 'Newsletter'
            when lower(utm_source_group) like '%gojek%'  and main_source<>'3rd Party' then 'Gojek'
            when lower(utm_source_group) like '%pushnotif%'  and main_source<>'3rd Party' then 'Pushnotif'
            when lower(utm_source_group) like '%banner%'  and main_source<>'3rd Party' then 'Banner'
            when lower(utm_source_group) like '%socmed%'  and main_source<>'3rd Party' then 'Socmed'
            when lower(utm_source_group) like '%whats%'  and main_source<>'3rd Party' then 'Whatsapp Share'
            else 'Organic'
            end as utm_source,
        case
            when lower(source_category_level1) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))=lower(coalesce(child_short_url, project_url)) then utm_campaign
            when lower(source_category_level1) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))<>lower(coalesce(child_short_url, project_url)) then 'Other Ads'
            end as utm_ad_name,
        project_categories_medical as project_categories_medical,
        project_url as parent_url,
        campaigner_full_name as campaigner_full_name,
        campaigner_organization_status as campaigner_organization_status,
        partners as partners,
        acquisition_by as acquisition_by,
        --split_part(split_part(cast(flag_details as text),',',8),':',2) as agent_optimize,
        JSON_EXTRACT_SCALAR(flag_details,'$.agent_name_ads_by_agency') agent_optimize,
        coalesce(child_short_url, project_url) as url_donation,
        date(verified) as verified_day, --to_date(to_char(verified, 'YYYY-MM-DD'), 'YYYY-MM-DD') as verified_day,
        cast(concat(extract(YEAR from verified),'-',extract(MONTH from verified)) as string) as verified_month -- to_char(verified, 'YYYY-MM') as verified_month
    from (select *, split(utm_campaign, '_') utm_campaign_1 from data_warehouse.f_donation) a
    where cast(flag_support_details as string) like '%"optimize_by_ads":true%'
    and (donation_statuses = 'VERIFIED' OR donation_statuses = 'PAID')
    and verified >= '2020-01-01'
    group by 5,6,7,8,9,10,11,12,13,14,15,16
),
-- ads_donation
ads_donation_1 as (
    Select
        agent_optimize as agent_optimize,
        project_categories_medical as project_categories_medical,
        parent_url as parent_url,
        campaigner_full_name as campaigner_full_name,
        campaigner_organization_status as campaigner_organization_status,
        partners as partners,
        acquisition_by as acquisition_by,
        pic_ad_name,
        last_ad_name,
        start_date_url,
        ads_source,
        ad_name,
        --preview_link,
        utm_ad_name,
        Coalesce(ad_name,utm_ad_name) as ads_name,
        Coalesce(utm_source,'no donation') as utm_source,
        Coalesce(verified_month,month_ads) as month_id,
        Coalesce(verified_day,date_ads) as date_id,
        --feeding_date_ads,
        Coalesce(url_donation,short_url_ads) as url_campaign,
        Coalesce(gdv,0) as gdv,
        Coalesce(trx,0) as trx,
        Coalesce(gdv_pwa,0) as gdv_pwa,
        Coalesce(gdv_apps,0) as gdv_apps,
        Coalesce(cost,0) as cost,
        Coalesce(landing_page_views,0) as landing_page_views,
        Coalesce(impressions,0) as impressions,
        Coalesce(action_link_click,0) as action_link_click,
        Coalesce(website_purchase,0) as website_purchase,
        Coalesce(purchase_conversion_value,0) as purchase_conversion_value
    from ads_2 full outer join donation_1
    on ads_2.ad_name = donation_1.utm_ad_name 
        and ads_2.date_ads = donation_1.verified_day
        and ads_2.short_url_ads = donation_1.url_donation
),
-- tbl_ga
ga as (
    select
        concat(url, "_", campaign, "_", date_id) as url_campaign_date,
        sum(page_views) as page_views
    from (select * from `kitabisa-data-team.data_warehouse.f_daily_ga` 
    where date_id >= '2020-01-01')
    group by 1
), 
ads_donation_permalink as (
    select 
    ads_donation_1.*,
    l_preview_link as preview_link,
    ads_preview_link.date_id as feeding_date_ads 
    from ads_donation_1 
    left join ads_preview_link 
    on trim(left(ads_donation_1.ads_name,120)) = trim(left(ads_preview_link.l_ad_name,120))
),
-- tbl ads_donation_ga
ads_donation_ga as (
    select 
    ads_donation_permalink.*,
    coalesce(page_views,0) as ga_page_views
    from ads_donation_permalink left join ga
    on concat(ads_donation_permalink.url_campaign, "_", left(ads_donation_permalink.ads_name,100), "_", ads_donation_permalink.date_id) = ga.url_campaign_date
),
-- tbl_proj

project_1 as (
    Select
        cast(project_id as string) as project_id,
        short_url as url_proj,
        acquisition_by,
        launched,
        expired,
        final_donation_percentage,
        tbl_hash.hash,
        tbl_hash.parent_user_id, 
        tbl_hash.parent_user_name
    from data_warehouse.f_project 
    left join 
    (
        select distinct parent_user_id,
        case when parent_user_id is not null then client_name else null end as parent_user_name,
        coalesce(safe_cast(c.user_id as int64),h.user_id) as campaigner_id,
        h.hash 
        from `kitabisa-data-team.data_lake.gsheet_ngo_cabang` c
        full outer join `kitabisa-data-team.data_warehouse.d_hash_user_dashboard_lembaga` h
        on safe_cast(parent_user_id as int64)=h.user_id
    where h.hash is not null
    ) as tbl_hash
    on f_project.user_id = tbl_hash.campaigner_id 
    ),
-- grouping campaign issue
campaign_issue as (
    SELECT
    -- campaign issue
    case 
        when lower(project_categories)="difabel" then 'difabel'
        when medical = true
            and (
                lower(short_url) like "%prematur%" 
                or lower(title) like "%prematur%"
                or lower(description) like "%prematur%"
                or lower(diagnosis) like "%prematur%"
                or lower(disease_tags) like "%prematur%"
            )
        then "prematur" 
        when medical = true
            and (
                (lower(diagnosis) like "%komplikasi%" or lower(disease_tags) like "%komplikasi%")
                or (lower(short_url) like "%komplikasi%" or lower(title) like "%komplikasi%" or lower(description) like "%komplikasi%")
            ) 
        then "komplikasi" 
        when medical = true
            and (
                (lower(diagnosis) like "%autoimun%" or lower(disease_tags) like "%autoimun%")
                or (lower(short_url) like "%autoimun%" or lower(title) like "%autoimun%" or lower(description) like "%autoimun%")    
            )
        then "autoimun" 
        when medical = true
            and (
                (lower(diagnosis) like "%jantung%" or lower(disease_tags) like "%kjantung%")
                or (lower(short_url) like "%jantung%" or lower(title) like "%jantung%")
            )
        then "jantung" 
        when medical = true 
            and (
                (lower(diagnosis) like "%kanker%" AND lower(diagnosis) like "%darah%")
                or (lower(disease_tags) like "%kanker%" AND lower(disease_tags) like "%darah%")
                or (lower(short_url) like "%kanker%" AND lower(short_url) like "%darah%")
                or (lower(title) like "%kanker%" AND lower(title) like "%darah%")
                or (lower(description) like "%kanker%" AND lower(description) like "%darah%")
            )
        then "kanker darah"
        when medical = true 
            and (
                (lower(diagnosis) like "%kanker%" or lower(diagnosis) like "%tumor%")
                or (lower(disease_tags) like "%kanker%" or lower(disease_tags) like "%tumor%")
                or (lower(short_url) like "%kanker%" or lower(short_url) like "%tumor%")
                or (lower(title) like "%kanker%" or lower(title) like "%tumor%")
                or (lower(description) like "%kanker%" or lower(description) like "%tumor%")
            )
        then "kanker/tumor"
        when medical = true 
            and (
                (lower(diagnosis) like "%hydro%" or lower(diagnosis) like "%hidro%")
                or (lower(disease_tags) like "%hydro%" or lower(disease_tags) like "%hidro%")
                or (lower(short_url) like "%hydro%" or lower(short_url) like "%hidro%")
                or (lower(title) like "%hydro%" or lower(title) like "%hidro%")
                or (lower(description) like "%hydro%" or lower(description) like "%hidro%")
            )
        then "hidrosefalus"
        when medical = true 
            and (
                (lower(diagnosis) like "%artesia bi%" or lower(diagnosis) like "%atresia bi%")
                or (lower(disease_tags) like "%artesia bi%" or lower(disease_tags) like "%atresia bi%")
                or (lower(short_url) like "%artesia bi%" or lower(short_url) like "%atresia bi%")
                or (lower(title) like "%artesia bi%" or lower(title) like "%atresia bi%")
            )
        then "atresia bilier"
        when medical = true 
            and (
                (lower(diagnosis) like "%cerebral palsy%" or lower(diagnosis) like "%lumpuh otak%")
                or (lower(disease_tags) like "%cerebral palsy%" or lower(disease_tags) like "%lumpuh otak%")
                or (lower(short_url) like "%cerebral palsy%" or lower(short_url) like "%lumpuh otak%")
                or (lower(title) like "%cerebral palsy%" or lower(title) like "%lumpuh otak%")
            )
        then "cerebral palsy"
        when medical = true 
            and (
                (lower(diagnosis) like "%meningitis%" or lower(diagnosis) like "%radang otak%")
                or (lower(disease_tags) like "%meningitis%" or lower(disease_tags) like "%radang otak%")
                or (lower(short_url) like "%meningitis%" or lower(short_url) like "%radang otak%")
                or (lower(title) like "%meningitis%" or lower(title) like "%radang otak%")
                or (lower(description) like "%meningitis%" or lower(description) like "%radang otak%")
            )
        then "meningitis"
        when medical = true 
            and (
                (lower(diagnosis) like "%obstruksi usus%" or lower(diagnosis) like "%invaginasi usus%" or lower(diagnosis) like "%lumpuh usus%")
                or (lower(disease_tags) like "%obstruksi usus%" or lower(disease_tags) like "%invaginasi usus%" or lower(disease_tags) like "%lumpuh usus%")
                or (lower(short_url) like "%obstruksi usus%" or lower(short_url) like "%invaginasi usus%" or lower(short_url) like "%lumpuh usus%")
                or (lower(title) like "%obstruksi usus%" or lower(title) like "%invaginasi usus%" or lower(title) like "%lumpuh usus%")
                or (lower(description) like "%obstruksi usus%" or lower(description) like "%invaginasi usus%" or lower(description) like "%lumpuh usus%")
            )
        then "obstruksi usus"
        when medical = true 
            and (
                (lower(diagnosis) like "%hipotiroid%" or lower(diagnosis) like "%gondok%")
                or (lower(disease_tags) like "%hipotiroid%" or lower(disease_tags) like "%gondok%")
                or (lower(short_url) like "%cerebral palsy%" or lower(short_url) like "%gondok%")
                or (lower(title) like "%hipotiroid%" or lower(title) like "%gondok%")
                or (lower(description) like "%hipotiroid%" or lower(description) like "%gondok%")
            )
        then "hipotiroid"
        when medical = true
            and 
            (
                (
                    (lower(diagnosis) like "%penyakit%" or lower(diagnosis) like "%kondisi%") 
                    and lower(diagnosis) like "%langka%"
                ) or
                (
                    (lower(short_url) like "%penyakit%" or lower(short_url) like "%kondisi%") 
                    and lower(short_url) like "%langka%"
                ) or
                (
                    (lower(title) like "%penyakit%" or lower(title) like "%kondisi%") 
                    and lower(title) like "%langka%"
                ) or
                (
                    (lower(description) like "%penyakit%" or lower(description) like "%kondisi%") 
                    and lower(description) like "%langka%"
                ) 
            )    
        then "penyakit langka"
        when medical = true 
            and (
                (lower(diagnosis) like "%nicu%" or lower(disease_tags) like "%nicu%" or lower(short_url) like "%nicu%")
                or (lower(title) like "%nicu%" AND title like "%PICU%")
                or (lower(description) like "%nicu%" AND description like "%PICU%")
            )
        then "NICU/PICU"
        when medical = true
            and (
                (lower(diagnosis) like "%kecelakaan%" or lower(disease_tags) like "%kecelakaan%")
                or (lower(short_url) like "%kecelakaan%" or lower(title) like "%kecelakaan%" or lower(description) like "%kecelakaan%")
            )
        then "kecelakaan"
        when medical = true
            and (
                (lower(diagnosis) like "%begal%" or lower(disease_tags) like "%begal%")
                or (lower(short_url) like "%begal%" or lower(title) like "%begal%" or lower(description) like "%begal%")
            )
        then "korban begal"
        WHEN medical = false
            and (LOWER(project_categories)="panti asuhan" OR LOWER(project_categories)="anak yatim dan panti asuhan")
		THEN "yatim"
		WHEN medical = false 
            and (LOWER(project_categories)="bencana alam" AND lower(short_url) not like "%corona%")
		THEN "bencana alam"
		WHEN medical = false 
            and (LOWER(project_categories)="infrastruktur umum" OR LOWER(project_categories)="rumah ibadah" OR  LOWER(project_categories)="masjid dan pesantren") 
            and (
                (lower(short_url) like "%masjid%" or lower(short_url) like "%mesjid%") 
                OR (lower(title) like "%masjid%" or lower(title) like "%mesjid%")
            )
            AND (lower(description) like "%masjid%" or lower(description) like "%mesjid%")
            THEN "masjid"
		WHEN medical = false
				and (LOWER(project_categories)="infrastruktur umum" OR LOWER(project_categories)="rumah ibadah") 
				AND (
                    (lower(short_url) like "%gereja%") 
				    OR (lower(title) like "%gereja%")
                )
				AND (lower(description) like "%gereja%")
				THEN "gereja"
		WHEN medical = false
				and (LOWER(project_categories)="infrastruktur umum" OR LOWER(project_categories)="rumah ibadah") 
				AND (
                    (lower(short_url) like "%sumur%" or lower(short_url) like "%sumber air%") 
				    OR (lower(title) like "%sumur%" or lower(title) like "%air%")
                )
				AND (lower(description) like "%sumur%" or lower(description) like "%air%")
				THEN "sumur"		
		WHEN medical = false
				and (LOWER(project_categories)="infrastruktur umum" OR LOWER(project_categories)="bantuan pendidikan")  
				AND (
                    (lower(short_url) like "%pesantren%" or lower(short_url) like "%rumah quran%" or lower(short_url) like "%rumah tahfidz%" or lower(short_url) like "%rumah tahfiz%") 
				    OR (lower(title) like "%pesantren%" or lower(title) like "%rumah quran%" or lower(title) like "%rumah tahfidz%" or lower(title) like "%rumah tahfiz%")
                )
				AND (lower(description) like "%pesantren%" or lower(description) like "%rumah quran%" or lower(description) like "%rumah tahfidz%" or lower(description) like "%rumah tahfiz%")
				THEN "pesantren/rumah quran/rumah tahfidz"
		WHEN medical = false
				and (LOWER(project_categories)="infrastruktur umum" OR LOWER(project_categories)="bantuan pendidikan")  
				AND (
                    (lower(short_url) like "%sekolah%" or lower(short_url) like "%sekolah%" or lower(short_url) like "%sekolah%" or lower(short_url) like "%sekolah%") 
				    OR (lower(title) like "%sekolah%" or lower(title) like "%sekolah%" or lower(title) like "%sekolah%" or lower(title) like "%sekolah%")
                )
				AND (lower(description) like "%sekolah%" or lower(description) like "%sekolah%" or lower(description) like "%sekolah%" or lower(description) like "%sekolah%")
				THEN "sekolah"
		WHEN medical = false
			and (LOWER(project_categories)="kemanusiaan" OR LOWER(project_categories)="kegiatan sosial" OR LOWER(project_categories)="mualaf dan hafidz quran") 
			AND (
                (lower(short_url) like "%hafidz%" or lower(short_url) like "%hafiz%" or lower(short_url) like "%hafal quran%" or lower(short_url) like "%hapal quran%") 
			    OR (lower(title) like "%hafidz%" or lower(title) like "%hafiz%" or lower(title) like "%hafal quran%" or lower(title) like "%hapal quran%")
            ) 
			AND (lower(description) like "%hafidz%" or lower(description) like "%hafiz%" or lower(description) like "%hafal quran%" or lower(description) like "%hapal quran%")  
			THEN "hafidz"
		WHEN medical = false
			and (LOWER(project_categories)="kemanusiaan" OR LOWER(project_categories)="kegiatan sosial" OR LOWER(project_categories)="orang tua dan dhuafa") 
			AND (
                (lower(short_url) like "%lansia%" or lower(short_url) like "%sebatang kara%" or lower(short_url) like "%kakek%" or lower(short_url) like "%nenek%") 
			    OR (lower(title) like "%lansia%" or lower(title) like "%sebatang kara%" or lower(title) like "%kakek%" or lower(title) like "%nenek%")
            )
			AND (lower(description) like "%lansia%" or lower(description) like "%sebatang kara%" or lower(description) like "%kakek%" or lower(description) like "%nenek%") 
			THEN "lansia"
		WHEN medical = false
			and (LOWER(project_categories)="kemanusiaan" OR LOWER(project_categories)="kegiatan sosial" OR LOWER(project_categories)="orang tua dan dhuafa") 
			AND (
                (lower(short_url) like "%sembako%" or lower(short_url) like "%pangan%" or lower(short_url) like "%bahan makanan%") 
			    OR (lower(title) like "%sembako%" or lower(title) like "%pangan%" or lower(title) like "%kakek%" or lower(title) like "%bahan makanan%")
            )
			AND (lower(description) like "%sembako%" or lower(description) like "%pangan%" or lower(description) like "%bahan makanan%") 
			THEN "sembako"
       else "others"
    end as campaign_issue,
    project_id,
    launched,
    short_url,
    start_date_activated,
    start_date_ads_by_agency,
    from `kitabisa-data-team.data_mart.dt_project_details`
),
-- tabel project_details
project_details as (
    select 
        project_1.*,
        pd.project_id as pd_project_id,
        pd.launched as pd_launched,
        pd.short_url as pd_short_url,
        pd.campaign_issue as campaign_issue,
        pd.start_date_activated as start_date_activated,
        pd.start_date_ads_by_agency as start_date_ads_by_agency,
    from project_1 left join campaign_issue as pd
    on cast(project_1.project_id as string) = cast(pd.project_id as string) and project_1.url_proj  = pd.short_url and project_1.launched = pd.launched
    where start_date_ads_by_agency is not null
),
-- funneling worklist growth
leads_grading as (
    SELECT 
        short_url,
        max(case when timestamp = '' then null  else cast(timestamp as date) end) as ready_date,
        max(grade) as leads_grading,
    FROM `kitabisa-data-team.data_lake.gsheet_funneling_worklist` 
    group by 1
),
project_details_grading as (
    select 
        *
    from project_details left join leads_grading
    on project_details.url_proj = leads_grading.short_url  
),
-- tbl_ads_donation_project 
ads_donation_project_1 as (
    Select
        month_id,
        date_id,
        campaign_issue,
        url_campaign,
        ready_date,
        feeding_date_ads,
        leads_grading,
        ads_name,
        preview_link,
        ads_source,
        utm_source,
        gdv,
        trx,
        gdv_pwa,
        gdv_apps,
        cost,
        impressions,
        action_link_click,
        website_purchase,
        purchase_conversion_value,
        project_id,
        parent_user_id,
        parent_user_name,
        launched,
        start_date_activated,
        start_date_ads_by_agency,
        expired,
        final_donation_percentage,
        landing_page_views,
        ga_page_views,
        Coalesce(Coalesce(pic_ad_name, first_value(pic_ad_name) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(last_ad_name) over(partition by url_campaign order by cost desc)) as pic_name,
        first_value(project_categories_medical) over(partition by url_campaign order by gdv desc) as project_categories_medical,
        first_value(parent_url) over(partition by url_campaign order by gdv desc) as parent_url,
        first_value(campaigner_full_name) over(partition by url_campaign order by gdv desc) as campaigner_full_name,
        first_value(campaigner_organization_status) over(partition by url_campaign order by gdv desc) as campaigner_organization_status,
        first_value(partners) over(partition by url_campaign order by gdv desc) as partners_page,
        first_value(agent_optimize) over(partition by url_campaign order by gdv desc) as agent_optimize,
        first_value(start_date_url) over(partition by url_campaign order by cost desc) as start_date_url,
        coalesce(first_value(ads_donation_ga.acquisition_by) over(partition by url_campaign order by gdv desc), project_details_grading.acquisition_by) as acquisition_by,
    from ads_donation_ga left join project_details_grading 
    on ads_donation_ga.url_campaign = project_details_grading.url_proj
),
-- Tabel ngo_region_id_2
region_ngo_id_2 as (
    Select * from (
    -- Tabel pairing region ngo list
        Select
            row_number() over (partition by project_id) as filter_row,
            cast(project_id as string) as project_region_ngo_list,
            regional_id as regional_id_list
        from data_lake.mn_paired_offline
    ) as region_ngo_id_1
    where filter_row=1
),
-- Tabel ads_donation_project_region_id_list
ads_donation_project_ngo_list_1 as (
    Select
        coalesce(trim(lower(split(pic_name,'_')[SAFE_OFFSET(4)])),agent_optimize) as pic_content,
        trim(lower(split(pic_name,'_')[SAFE_OFFSET(5)])) as pic_visual,
        trim(lower(split(pic_name,'_')[SAFE_OFFSET(6)])) as pic_dm,
        case when acquisition_by='program_acquisition' then coalesce(partners_page,parent_url)
            when (acquisition_by='partnership_acquisition' OR acquisition_by='non-partnership only' OR acquisition_by like '%hospital%') AND campaigner_organization_status='PERSONAL' then 'PERSONAL'
            else campaigner_full_name
            end as partner_client,
        regional_id_list,
        month_id,
        date_id,
        campaign_issue,
        url_campaign,
        ready_date,
        feeding_date_ads,
        leads_grading,
        ads_name,
        preview_link,
        ads_source,
        utm_source,
        gdv,
        trx,
        gdv_pwa,
        gdv_apps,
        agent_optimize,
        cost,
        landing_page_views,
        ga_page_views,
        impressions,
        action_link_click,
        website_purchase,
        purchase_conversion_value,
        project_id,
        parent_user_id,
        parent_user_name,
        launched,
        start_date_activated,
        start_date_ads_by_agency,
        expired,
        final_donation_percentage,
        project_categories_medical,
        parent_url,
        campaigner_full_name,
        campaigner_organization_status,
        partners_page,
        start_date_url,
        acquisition_by
    from ads_donation_project_1  left join region_ngo_id_2 
    on ads_donation_project_1.project_id = region_ngo_id_2.project_region_ngo_list
),
-- Tabel database region ngo
region_ngo_province_1 as (
	Select
		region_id,
		provinsi as regional_province_name
	from data_lake.gsheet_mapping_region_location
)
-- output supermetrics_pic

    Select
        pic_content,
        pic_visual,
        pic_dm,
        Coalesce(b.squad,
        (case when acquisition_by like '%hospital%' then 'Hospital'
            when acquisition_by='program_acquisition' OR (acquisition_by='zakat_acquisition' AND campaigner_full_name like '%Kitabisa%') OR ((campaigner_full_name='Peduli Anak Foundation' OR campaigner_full_name='RQV Indonesia') AND date_id <= '2021-03-31') OR campaigner_full_name='UNHCR Indonesia' OR (campaigner_full_name='Yayasan Bina Mulia Bojonegoro' AND date_id < '2021-04-01') OR ((campaigner_full_name like '%an Ash-Shalihin%' OR campaigner_full_name='Pondok Sedekah Indonesia' OR campaigner_full_name='Pondok Sedekah Sulsel') AND date_id <= '2021-01-31') THEN 'Program, Zakat, & NGO non-Region'
            when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') AND (regional_province_name='Aceh' OR regional_province_name='Lampung' OR regional_province_name='Banten' OR regional_province_name='DKI Jakarta' OR regional_province_name='Jambi' OR regional_province_name='Kalimantan Barat' OR regional_province_name='Riau' OR regional_province_name='Sumatera Barat' OR regional_province_name='Sumatera Selatan' OR regional_province_name='Sumatera Utara') THEN 'NGO Barat 1'
            when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') AND (regional_province_name='Bengkulu' OR regional_province_name='Jawa Barat' OR regional_province_name='Kalimantan Selatan' OR regional_province_name='Kalimantan Tengah' OR regional_province_name='Kalimantan Timur' OR regional_province_name='Kalimantan Utara' OR regional_province_name='Kep. Bangka Belitung' OR regional_province_name='Kep. Riau') THEN 'NGO Barat 2'
            when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') AND (regional_province_name='Bali' OR regional_province_name='Gorontalo' OR regional_province_name='Jawa Tengah' OR regional_province_name='Nusa Tenggara Barat' OR regional_province_name='Nusa Tenggara Timur' OR regional_province_name='Sulawesi Tengah' OR regional_province_name='Yogyakarta') THEN 'NGO Timur 1'
            when (acquisition_by like '%small%' OR acquisition_by like '%impacts%') AND (regional_province_name='Jawa Timur' OR regional_province_name='Sulawesi Tenggara' OR regional_province_name='Sulawesi Utara' OR regional_province_name='Maluku' OR regional_province_name='Maluku Utara' OR regional_province_name='Papua' OR regional_province_name='Papua Barat' OR regional_province_name='Sulawesi Barat' OR regional_province_name='Sulawesi Selatan') THEN 'NGO Timur 2'
            when acquisition_by like '%non-partnership%' OR ((acquisition_by like '%influencer%' OR acquisition_by like '%partnership_ac%') AND date_id > '2021-01-31') then 'In-Bound & Influencer'
            end),
        (case
            when agent_optimize like '%Shanara%' OR agent_optimize like '%Andaris%' OR agent_optimize like '%Dewi Marisa%' OR agent_optimize like '%Inta Yunita%' OR agent_optimize like '%Aldila%' OR agent_optimize like '%Davin%' OR agent_optimize like '%Rafianti%' OR agent_optimize like '%Annisa Dwi%' OR agent_optimize like '%Shintia%' OR agent_optimize like '%Alya%' OR agent_optimize like '%Tasha%' THEN 'NGO Barat 1'
            when agent_optimize like '%Fania%' OR agent_optimize like '%Alega%' OR agent_optimize like '%Lutfiah%' OR agent_optimize like '%Tata%' OR agent_optimize like '%Nadira%' OR agent_optimize like '%Emilia%' OR agent_optimize like '%Yuliana%' THEN 'NGO Barat 2'
            when agent_optimize like '%Dimas%' OR agent_optimize like '%Vora%' OR agent_optimize like '%Ghilman%' OR agent_optimize like '%Sadida%' OR agent_optimize like '%Vicky%' OR agent_optimize like '%Fajar%' THEN 'NGO Timur 1'
            when agent_optimize like '%Gita%' OR agent_optimize like '%Unike%' OR agent_optimize like '%Clara%' OR agent_optimize like '%Fira Shabrina%' OR agent_optimize like '%Tazkiya%' OR agent_optimize like '%Medina%' OR agent_optimize like '%Nurullita%' THEN 'NGO Timur 2'
            when agent_optimize like '%Nur Rahmah%' OR agent_optimize like '%Nadia Aisha%' OR agent_optimize like '%Dwi Astri%' OR agent_optimize like '%Nabil%' OR agent_optimize like '%Naya%' OR agent_optimize like '%Winny%' OR agent_optimize like '%Christabella%' OR agent_optimize like '%Oktaviani%' THEN 'Program, Zakat, & NGO non-Region'
            when agent_optimize like '%Monica%' OR agent_optimize like '%Lindyra%' OR agent_optimize like '%Dela Destri%' THEN 'In-Bound & Influencer'
            when agent_optimize like '%Gabriel%' OR agent_optimize like '%Zahra Matarani%' OR agent_optimize like '%Adinda%' OR agent_optimize like '%Dwi Ayu%' THEN 'Hospital'
            end)) as squad,
        partner_client,
        regional_province_name,
        month_id,
        date_id,
        url_campaign,
        ads_donation_project_ngo_list_1.project_id,
        campaign_issue,
        leads_grading,
        ready_date,
        feeding_date_ads,
        ads_name,
        preview_link,
        ads_source,
        utm_source,
        gdv,
        trx,
        gdv_pwa,
        gdv_apps,
        cost,
        landing_page_views,
        ga_page_views,
        impressions,
        action_link_click,
        website_purchase,
        purchase_conversion_value,
        launched,
        start_date_activated,
        start_date_ads_by_agency,
        expired,
        final_donation_percentage,
        project_categories_medical,
        parent_url,
        campaigner_full_name,
        campaigner_organization_status,
        parent_user_name,
        partners_page,
        start_date_url,
        acquisition_by
    from ads_donation_project_ngo_list_1 left join region_ngo_province_1
    on cast(ads_donation_project_ngo_list_1.regional_id_list as string) = region_ngo_province_1.region_id
    left join `kitabisa-data-team.data_lake.gsheet_pof_campaign` b
    on ads_donation_project_ngo_list_1.url_campaign=b.campaign
