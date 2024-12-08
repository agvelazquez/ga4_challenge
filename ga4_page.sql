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
)

--select * from stg_events

, stg_session_start_end as (
select
  concat(cast(ga_session_id as string),'-',cast(user_pseudo_id as string)) as sessions_id, 
  min(event_timestamp) as event_start,
  max(event_timestamp) as event_end
from stg_events
group by 
  1
)

, stg_landing_exit as (
select 
  sse.sessions_id, 
  min(e1.event_page) as landing_page,
  min(e2.event_page) as exit_page
from stg_session_start_end sse
left join stg_events e1
    on sse.event_start = e1.event_timestamp
left join stg_events e2
    on sse.event_end = e2.event_timestamp
group by 1
)

, final as (
select 
  e.event_dt, 
  e.event_page, 
  concat(cast(e.ga_session_id as string),'-',cast(e.user_pseudo_id as string)) as sessions_id, 
  min(e.event_ts) as session_start_timestamp,
  max(e.event_ts) as session_end_timestamp,
  timestamp_diff(max(e.event_ts), min(e.event_ts), second) as session_page_duration_in_sec, 
  sum(case when e.event_page = e.landing_page then 1 else 0 end) as entrances,
  sum(case when e.event_page = e.exit_page then 1 else 0 end) as exits
from stg_events e
group by 1,2,3
--order by 6 desc
)

select 
  event_page, 
  sum(session_page_duration_in_sec) as total_time_on_page,
  avg(session_page_duration_in_sec) as avg_time_on_page, 
  sum(entrances) as entrances,
  sum(exits) as exits
from final
group by 
  1
