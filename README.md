**BigQuery/Dataform Developer for Ecommerce**

In Dataform, create a process to build a session level table:

1. Use GA4 raw table source_tables_USER_INDEX.source_ga4_events as source of data, see below for how to find your USER_INDEX *
2. Resulting dataform step must be named ga4_stg01_sessions.sqlx
3. Resulting table must have following fields:
- session_id, user_pseudo_id, device, date, session_start_timestamp, source(from event_params), medium(from event_params), campaign(from event_params), landing_page, exit_page, session_duration_in_sec, is_session_engaged
4. is_session_engaged should return TRUE when at least  one of the below conditions are met:
- session had a purchase 
- session had 2 or more view_item events 
- session lasted 10 seconds or more

Resulting table must be session level, i.e. exactly one row for each session_id 

In Dataform, create a process to build a page level table:


1. Use GA4 raw table source_tables_USER_INDEX.source_ga4_events as source of data *
2. Resulting dataform step must be named ga4_stg02_pages.sqlx
3. Resulting table must have following fields:
- date, page_url, total_time_on_page, avg_time_on_page, entrances, exits

In Dataform, create a process to add shopify product collection name to source_shopify_orderlines_stg01 table

Use the following tables for collections data/mapping: 
- source_tables_USER_INDEX.source_shopify_orderlines_stg01 *
- source_tables_USER_INDEX.source_shopify_collects *
- source_tables_USER_INDEX.source_shopify_smart_collections *
- source_tables_USER_INDEX.source_shopify_custom_collections *

If one product is part of multiple collections, add all collections in alphabetical order with comma as delimiter
- Resulting dataform step must be named shopify_orderlines_stg02_with_collection.sqlx
- Resulting table shopify_orderlines_stg02_with_collection must at order line level,  i.e. exactly one row for each order line

Provide a short overview on how you verified the resulting tables from tasks 1-3 produce correct data.  

* USER_INDEX is unique per user and can be found in your Dataform, 
See the below screenshot, the dataset for the below user should be source_tables_30403

