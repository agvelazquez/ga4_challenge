with event_param as (
select 
  event_timestamp, 
  min(case when ep.key = 'ga_session_id' then ep.value.int_value end) as ga_session_id, 
  min(case when ep.key = 'ga_session_number' then ep.value.int_value end) as ga_session_number,
  min(case when ep.key = 'page_location' then ep.value.string_value end) as event_page,
  min(case when ep.key = 'campaign' then ep.value.string_value end) as event_campaign,
  min(case when ep.key = 'source' then ep.value.string_value end) as event_source,
  min(case when ep.key = 'medium' then ep.value.string_value end) as event_medium,
from ecomtest-767676.source_tables_64598.source_ga4_events e
left join unnest(event_params) ep 
  on true
  where 
    1=1
group by 
  1
)

, stg_events as (
select 
  e.event_timestamp, 
  TIMESTAMP_MICROS(e.event_timestamp) as event_ts,
  PARSE_DATE('%Y%m%d', e.event_date) AS event_dt,
  ep.ga_session_id, 
  ep.ga_session_number,
  ep.event_page,
  e.event_name,
  e.user_id,
  e.user_pseudo_id, 
  e.event_value_in_usd,
  device.category as device,
  first_value(ep.event_medium) over(partition by ga_session_id order by TIMESTAMP_MICROS(e.event_timestamp) asc) as session_medium,
  first_value(ep.event_source) over(partition by ga_session_id order by TIMESTAMP_MICROS(e.event_timestamp) asc) as session_source,
  case when e.event_name = 'purchase' then True else False end as is_purchase_event,
  case when e.event_name = 'view_item' then True else False end as is_view_item_event, 
  first_value(event_page) over(partition by ga_session_id order by TIMESTAMP_MICROS(e.event_timestamp) asc) as landing_page,
  first_value(event_page) over(partition by ga_session_id order by TIMESTAMP_MICROS(e.event_timestamp) desc) as exit_page
from ecomtest-767676.source_tables_64598.source_ga4_events e
left join event_param ep 
  on e.event_timestamp= ep.event_timestamp
where 
  1=1
--order by event_ts asc
)

--select * from stg_events

, final as (
select  --session_id, count(distinct event_timestamp ) 
  concat(cast(ga_session_id as string),'-',cast(user_pseudo_id as string)) as sessions_id, 
  min(device) as session_device,
  min(event_dt) as session_start_dt, 
  min(event_ts) as session_start_timestamp,
  min(landing_page) as landing_page, 
  min(exit_page) as exit_page,
  min(session_medium) as session_medium,
  min(session_source) as session_source,
  timestamp_diff(max(event_ts), min(event_ts), second) as session_duration_in_sec,
  string_agg(distinct event_name, ', ') as events, 
  count(distinct event_timestamp) as number_unique_events, 
  sum(case when is_purchase_event = True then 1 else 0 end) as purchase_events, 
  sum(case when is_view_item_event = True then 1 else 0 end) as view_item_events, 
from stg_events 
--where ga_session_id = 1725188430 
--group by 1 order by 2 desc
group by 1
order by 1 asc
)

select 
  sessions_id,
  session_device,
  session_start_dt,
  session_start_timestamp,
  landing_page,
  exit_page, 
  session_medium,
  session_source,
  session_duration_in_sec,
  case when purchase_events > 0 or view_item_events >= 2 or session_duration_in_sec >= 10 then True else false end as is_session_engaged
from 
  final 

--select * from stg_events where ga_session_id = 1725166648 --where event_timestamp = 1725167170784876
--where ga_session_id = 1725166648
--select * from final where sessions_id = '1725166648-966573989.1725166649'

