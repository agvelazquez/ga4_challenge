with stg_product_collection as (
select 
  product_id, 
  --collection_id, 
  --cc.title as custom_collection_name,
  --sm.title as smart_collection_name, 
  string_agg(coalesce(cc.title, sm.title), ', ') as collection_names
from ecomtest-767676.source_tables_64598.source_shopify_collects c 
left join ecomtest-767676.source_tables_64598.source_shopify_custom_collections cc
  on c.collection_id = cc.id
left join ecomtest-767676.source_tables_64598.source_shopify_smart_collections sm
  on c.collection_id = sm.id
group by 1

) 

select * 
from ecomtest-767676.source_tables_64598.source_shopify_orderlines_stg01 ol 
left join stg_product_collection pc 
  on cast(ol.orderline_product_id as int64) = pc.product_id
