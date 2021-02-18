-- Omez Campaign Performance
-- Hive
set hive.groupby.orderby.position.alias=true ;
select cm.campaign_id, cm.channel as campaign_type, cm.campaign_name, ns.dt as notification_sent_date, nc.dt as notification_clicked_date, cm.campaign_vertical,
	count(distinct ns.profile_objectid) as reached_customers,
	count(distinct nc.profile_objectid) as clicked_customers,
	count(distinct case when pos.order_id is null then pos.id else pos.order_id end) as orders_placed_customers
from (select event_props_campaign_type, event_props_campaign_id, profile_identity, profile_objectid,dt,created_at
		from pe_consumer_ct.notification_sent_snapshot 
		where dt>= '2020-11-01')ns 
inner join (select * from pre_analytics.campaign_metadata where campaign_metadata.campaign_name in ("Blog_Omez_Article1","Blog_Omez_Article2","Blog_Omez_Article3", "Blog_Omez_Article4", "Blog_Omez_Article5", "Blog_Omez_Article6","Blog_Omez_Article7","Blog_Omez_Article8", "Blog_Omez_Article9", "Blog_Omez_Article10","Blog_Omez_Article11", "Blog_Omez_Article12")) cm on cm.campaign_id = ns.event_props_campaign_id
left join (select dt, profile_objectid, cast(split(event_props_wzrk_id,'_')[0] as BIGINT) as campaign_id,
		   	profile_identity,created_at
			from pe_consumer_ct.notification_clicked_snapshot 
			where dt>= '2020-11-01' )nc on (ns.profile_objectid = nc.profile_objectid 
			and nc.campaign_id = ns.event_props_campaign_id and event_props_campaign_type != 'SMS'
			and ((UNIX_TIMESTAMP(nc.created_at)-UNIX_TIMESTAMP(ns.created_at))/3600)between 0 and 32)
left join (select cast(split(get_json_object(event_props_json, '$.campaign_id'),'_')[0] as BIGINT) as campaign_id, 
			get_json_object(event_props_json, '$.id') as id,
		   get_json_object(event_props_json, '$.order_id') as order_id,
			profile_objectid, created_at
			from  pe_consumer_ct.c_placed_order_snapshot 
			where dt>= '2020-11-01')pos on (ns.event_props_campaign_id = pos.campaign_id and ns.profile_objectid = pos.profile_objectid)
where ns.event_props_campaign_type != 'SMS' and cm.campaign_vertical != 'Diagnostic'
group by cm.campaign_id, cm.channel, cm.campaign_name, ns.dt, nc.dt, cm.campaign_vertical
union all
select cm.campaign_id, cm.channel as campaign_type, cm.campaign_name, ns.dt as notification_sent_date, af.part_date as notification_clicked_date, cm.campaign_vertical,
	count(distinct ns.profile_objectid) as reached_customers,
	count(distinct af.customer_id) as clicked_customers,
	count(distinct foc.customer_id) as orders_placed_customers
from (select event_props_campaign_type, event_props_campaign_id, profile_identity, profile_objectid,dt,created_at
		from pe_consumer_ct.notification_sent_snapshot 
		where dt>= '2020-11-01')ns 
inner join (select * from pre_analytics.campaign_metadata where campaign_metadata.campaign_name in ("Blog_Omez_Article1","Blog_Omez_Article2","Blog_Omez_Article3", "Blog_Omez_Article4", "Blog_Omez_Article5", "Blog_Omez_Article6","Blog_Omez_Article7","Blog_Omez_Article8", "Blog_Omez_Article9", "Blog_Omez_Article10","Blog_Omez_Article11", "Blog_Omez_Article12")) cm on cm.campaign_id = ns.event_props_campaign_id
left join (select media_source, cam.customer_id, aeas.part_date, aeas.event_time
		from 
		(select media_source, appsflyer_id, event_time, dt as part_date
		from pe_consumer_af_android.in_app_events_android_snapshot aeas
		where lower(aeas.media_source) like '%%sms%%' and aeas.dt>= '2020-11-01'
		union all
		select media_source, appsflyer_id, event_time, dt
		from pe_consumer_af_ios.in_app_events_ios_snapshot aeis
		where lower(aeis.media_source) like '%%sms%%' and aeis.dt>= '2020-11-01') aeas
		inner join pe_pe2_pe2.customer_appsflyer_mapping_snapshot cam on aeas.appsflyer_id = cam.appsflyer_id
		) af on (ns.profile_identity = af.customer_id 
							and ((UNIX_TIMESTAMP(af.event_time)-UNIX_TIMESTAMP(ns.created_at))/3600)between 0 and 32)
left join (select id, customer_id,time_stamp, dt
			from pe_pe2_pe2.order_snapshot
			where dt>= '2020-11-01')foc on (af.customer_id = foc.customer_id  and af.customer_id is not null and
											((UNIX_TIMESTAMP(foc.time_stamp)-UNIX_TIMESTAMP(af.event_time))/3600)between 0 and 32)
where ns.event_props_campaign_type = 'SMS' and cm.campaign_vertical != 'Diagnostic'
group by cm.campaign_id, cm.channel, cm.campaign_name, ns.dt, af.part_date, cm.campaign_vertical;