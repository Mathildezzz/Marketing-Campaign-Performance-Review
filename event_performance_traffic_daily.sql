delete tutorial.event_performance_traffic_daily;
insert into tutorial.event_performance_traffic_daily


--所有活动都只要内场
WITH traffic_table AS (
    SELECT mcl.event_name
        ,mcl.sub_event_name
        ,'内场' as event_store_type
        ,mcl.wecom_tag
        ,mcl.event_start_dt
        ,mcl.event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code 
        ,mcl.city_cn as event_store_city
        ,mcl.distributor_name as event_store_distributor_name
        ,date(date_id)as date_id
        ,traffic_table.lego_store_code
        ,CAST(SUM(traffic_amt) AS FLOAT)               AS traffic
    FROM dm.agg_final_sales_by_store_daily traffic_table
    left join  
        (select * 
        from 
            tutorial.marketing_campaign_info_base_table_v2
        where event_store_type='内场') mcl
        ---traffic只有内场
        on UPPER(traffic_table.lego_store_code) = UPPER(mcl.original_store_code)
        and DATE(traffic_table.date_id) >= date(mcl.event_start_dt)
        AND DATE(traffic_table.date_id) <= date(mcl.event_end_dt)
         
    WHERE agg_type = 'LEGO'
        and distributor <>'LBR'
        and date(date_id)>='2023-01-01'
        
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

select 
    * 
    ,to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date
    ,getdate()                                                   AS dl_load_time
from traffic_table;