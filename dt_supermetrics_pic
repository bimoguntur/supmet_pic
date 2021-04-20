Select
 pic_content,
 pic_visual,
 pic_dm,
 Coalesce(
	(case when acquisition_by like '%hospital%' then 'Hospital'
		when acquisition_by='program_acquisition' OR (acquisition_by='zakat_acquisition' AND campaigner_full_name like '%Kitabisa%') OR ((campaigner_full_name='Peduli Anak Foundation' OR campaigner_full_name='RQV Indonesia') AND date_id <= '2021-03-31') OR (campaigner_full_name='UNHCR Indonesia' OR campaigner_full_name='Yayasan Bina Mulia Bojonegoro') OR ((campaigner_full_name like '%an Ash-Shalihin%' OR campaigner_full_name='Pondok Sedekah Indonesia' OR campaigner_full_name='Pondok Sedekah Sulsel') AND date_id <= '2021-01-31') THEN 'Program, Zakat, & NGO non-Region'
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
 ads_name,
 utm_source,
 gdv,
 trx,
 cost,
 landing_page_views,
 impressions,
 action_link_click,
 website_purchase,
 purchase_conversion_value,
 expired,
 final_donation_percentage,
 project_categories_medical,
 parent_url,
 campaigner_full_name,
 campaigner_organization_status,
 partners_page,
 start_date_url,
 acquisition_by
from
-- Tabel ads_donation_project_region_id_list
(
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
		url_campaign,
		ads_name,
		utm_source,
		gdv,
		trx,
		agent_optimize,
		cost,
		landing_page_views,
		impressions,
		action_link_click,
		website_purchase,
		purchase_conversion_value,
		project_id,
		expired,
		final_donation_percentage,
		project_categories_medical,
		parent_url,
		campaigner_full_name,
		campaigner_organization_status,
		partners_page,
		start_date_url,
		acquisition_by
	from
-- Tabel ads_donation_project
	(
		Select
			month_id,
			date_id,
			url_campaign,
			ads_name,
			utm_source,
			gdv,
			trx,
			cost,
			landing_page_views,
			impressions,
			action_link_click,
			website_purchase,
			purchase_conversion_value,
			project_id,
			expired,
			final_donation_percentage,
			Coalesce(Coalesce(pic_ad_name, first_value(pic_ad_name) over(partition by url_campaign,month_id order by month_id asc,cost desc)),first_value(last_ad_name) over(partition by url_campaign order by cost desc)) as pic_name,
			first_value(project_categories_medical) over(partition by url_campaign order by gdv desc) as project_categories_medical,
			first_value(parent_url) over(partition by url_campaign order by gdv desc) as parent_url,
			first_value(campaigner_full_name) over(partition by url_campaign order by gdv desc) as campaigner_full_name,
			first_value(campaigner_organization_status) over(partition by url_campaign order by gdv desc) as campaigner_organization_status,
			first_value(partners) over(partition by url_campaign order by gdv desc) as partners_page,
			first_value(agent_optimize) over(partition by url_campaign order by gdv desc) as agent_optimize,
			first_value(start_date_url) over(partition by url_campaign order by cost desc) as start_date_url,
			first_value(acquisition_by) over(partition by url_campaign order by gdv desc) as acquisition_by
		from
		-- Table ads & donation without ngo province
		-- tbl_ads_donation_1
		(
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
				Coalesce(ad_name_ads,utm_ad_name) as ads_name,
				Coalesce(utm_source,'no donation') as utm_source,
				Coalesce(verified_month,month_ads) as month_id,
				Coalesce(verified_day,date_ads) as date_id,
				Coalesce(url_donation,short_url_ads) as url_campaign,
				Coalesce(gdv,0) as gdv,
				Coalesce(trx,0) as trx,
				Coalesce(cost,0) as cost,
				Coalesce(landing_page_views,0) as landing_page_views,
				Coalesce(impressions,0) as impressions,
				Coalesce(action_link_click,0) as action_link_click,
				Coalesce(website_purchase,0) as website_purchase,
				Coalesce(purchase_conversion_value,0) as purchase_conversion_value
			from
			-- Table ads with pic
			-- tbl_ads_2
			(
				Select
					date_ads as date_ads,
					ad_name_ads,
					start_date_url,
					short_url_ads as short_url_ads,
					month as month_ads,
					start_date_ads,
					first_value(ad_name_ads) over(partition by short_url_ads,month order by month asc, start_date_ads desc) as pic_ad_name,
					first_value(ad_name_ads) over(partition by short_url_ads order by start_date_ads desc) as last_ad_name,
					sum(cost) as cost,
					sum(landing_page_views) as landing_page_views,
					sum(impressions) as impressions,
					sum(action_link_click) as action_link_click,
					sum(website_purchase) as website_purchase,
					sum(purchase_conversion_value) as purchase_conversion_value
				FROM
------------ Table ads only
				( --tbl_ads_1
					SELECT
						date as date_ads,
						left(ad_name,100) as ad_name_ads,
						short_url as short_url_ads,
						concat(cast(EXTRACT(YEAR FROM date) as string),'-',cast(EXTRACT(MONTH FROM date) as string)) as month, --to_char(date,'YYYY-MM') as month,
						min(date) over (partition by left(ad_name,100) order by date asc) as start_date_ads,
						min(date) over (partition by short_url order by date asc) as start_date_url,
						cost as cost,
						landing_page_views as landing_page_views,
						impressions as impressions,
						action_link_click as action_link_click,
						website_purchase as website_purchase,
						purchase_conversion_value as purchase_conversion_value
					FROM data_warehouse.f_supermetrics_facebook_ads
					where date >= '2020-01-01'
				) as ads_1
				group by 1,2,3,4,5,6
			) as ads_2
			full outer join
			-- Table donation only
			(
				SELECT
					sum(amount) as gdv,
					count(amount) as trx,
					case when lower(utm_source_group) like '%ads%'  and main_source<>'3rd Party' then 'Ads'
						when lower(utm_source_group) like '%newsletter%'  and main_source<>'3rd Party' then 'Newsletter'
						when lower(utm_source_group) like '%gojek%'  and main_source<>'3rd Party' then 'Gojek'
						when lower(utm_source_group) like '%pushnotif%'  and main_source<>'3rd Party' then 'Pushnotif'
						when lower(utm_source_group) like '%banner%'  and main_source<>'3rd Party' then 'Banner'
						when lower(utm_source_group) like '%socmed%'  and main_source<>'3rd Party' then 'Socmed'
						when lower(utm_source_group) like '%whats%'  and main_source<>'3rd Party' then 'Whatsapp Share'
						else 'Organic'
						end as utm_source,
					case
                        when lower(utm_source_group) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))=lower(coalesce(child_short_url, project_url)) then utm_campaign
						when lower(utm_source_group) like '%ads%'  and main_source<>'3rd Party' and trim(lower(utm_campaign_1[safe_offset(1)]))<>lower(coalesce(child_short_url, project_url)) then 'Other Ads'
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
				group by 3,4,5,6,7,8,9,10,11,12,13,14
			) as donation_1
			on ads_2.ad_name_ads = donation_1.utm_ad_name and ads_2.date_ads = donation_1.verified_day
		) as ads_donation_1
		left join
-- Tabel project only
		(
			Select
				cast(project_id as string) as project_id,
				short_url as url_proj,
				expired,
				final_donation_percentage
			from data_warehouse.f_project
		) as project_1 on ads_donation_1.url_campaign = project_1.url_proj
	) as ads_donation_project_1
	left join
-- Tabel pairing region ngo list filtered
	(
		Select *
		from
-- Tabel pairing region ngo list
		(
			Select
				row_number() over (partition by project_id) as filter_row,
				cast(project_id as string) as project_region_ngo_list,
				regional_id as regional_id_list
			from data_lake.mn_paired_offline
		) as region_ngo_id_1
		where filter_row=1
	) as region_ngo_id_2 on ads_donation_project_1.project_id = region_ngo_id_2.project_region_ngo_list
) as ads_donation_project_ngo_list_1
left join
-- Tabel database region ngo
(
	Select
		region_id,
		provinsi as regional_province_name
	from data_lake.gsheet_mapping_region_location
) as region_ngo_province_1 on cast(ads_donation_project_ngo_list_1.regional_id_list as string) = region_ngo_province_1.region_id
