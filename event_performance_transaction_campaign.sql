
-----------------4.交易表 only 活动-----------------
delete tutorial.event_performance_transaction_campaign;
insert into tutorial.event_performance_transaction_campaign

 with new_member as (
     SELECT distinct
        mcl.event_name
        ,mcl.sub_event_name
        ,mcl.wecom_tag
        ,mcl.event_start_dt
        ,mcl.event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code
        ,md.member_detail_id
        ,DATE(join_time) as join_date
        ,md.eff_reg_store
     FROM edw.d_member_detail md
     left join 
         (select * 
        from 
            tutorial.marketing_campaign_info_base_table_v2
        where event_store_type='内场') mcl
        on UPPER(md.eff_reg_store) = UPPER(mcl.reg_store_code) ----ori内场 lego外场 ori包含内外场所有newmember
        AND DATE(join_time) >= date(mcl.event_start_dt)
        AND DATE(join_time) <= date(mcl.event_end_dt)
     WHERE 
        mcl.original_store_code is not null
        and DATE(join_time) is not null
        -- and member_detail_id='11412310'
 )

,sales_shopper AS (
    SELECT distinct
        '外场'as event_store_type
        ,mcl.event_name
        ,mcl.sub_event_name
        ,mcl.wecom_tag
        ,date(mcl.event_start_dt) as event_start_dt
        ,date(mcl.event_end_dt) as event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code,
        trans.lego_store_code,
        trans.original_store_code,
        trans.date_id,
        trans.crm_member_id,
        trans.distributor_name,
        trans.if_eff_order_tag,
        trans.original_order_id,
        wr.join_date as  join_time,
        case when trans.crm_member_id IS NOT NULL then '1' else '0' end                                 as is_member,
        CASE WHEN wr.member_detail_id IS NOT NULL THEN '1' ELSE '0' END                                 AS is_new_member,
        sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)         AS sales,
        sum(case when crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS mbr_sales,
        sum(case when wr.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when wr.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS new_mbr_sales,
        sum(case when wr.member_detail_id is null and crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when wr.member_detail_id is null and crm_member_id IS NOT NULL  AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS existing_mbr_sales,
        
        -- order_rrp_amt AS sales
        COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE and crm_member_id is not null THEN crm_member_id ELSE NULL END)                AS ttl_member_shopper,
        SUM(CASE WHEN crm_member_id IS NOT NULL THEN sales_qty ELSE 0 END)                                                                   AS pieces
        ,SUM(CASE WHEN wr.member_detail_id is not null THEN sales_qty ELSE 0 END)                                                      AS new_mbr_pieces,
         SUM(CASE WHEN wr.member_detail_id is null and crm_member_id IS NOT NULL  THEN sales_qty ELSE 0 END) AS existing_mbr_pieces
        -- COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE and trans.crm_member_id IS NOT NULL THEN trans.original_order_id ELSE NULL END)      AS mbr_order_cnt
    FROM edw.f_member_order_detail trans
    inner join  (
        select * 
        from 
            tutorial.marketing_campaign_info_base_table_v2
        where event_store_type='外场'
        )mcl
        on UPPER(trans.lego_store_code) = UPPER(mcl.lego_store_code)
        and UPPER(trans.original_store_code) = UPPER(mcl.original_store_code)
        --这里的链接条件是订单相关，存在实际上无外场销售的活动，在维表中lego&original都是空，取不到这类活动外场的销售是正常的
        
        and DATE(trans.date_id) >= date(mcl.event_start_dt)
        AND DATE(trans.date_id) <= date(mcl.event_end_dt)
       
    left join new_member wr --内外场注册的新都算新，在外场下单就算外场的new shopper
         ON trans.crm_member_id::integer = wr.member_detail_id::integer
        -- and trans.lego_store_code=new_member.eff_reg_store
        and mcl.sub_event_name=wr.sub_event_name
    
    -- left join tutorial.event_performance_wechat_register wr --只有在外场注册&外场下单才算外场newshopper
    --     on trans.crm_member_id::integer = wr.member_detail_id::integer
    --     and wr.event_store_type='外场'
        --外场注册表，外场是否为newmember用这张表来判断
    WHERE is_rrp_sales_type = 1
        
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

 
,sales_shopper_2 AS (
    SELECT distinct
        '内场'as event_store_type
        ,mcl.event_name
        ,mcl.sub_event_name
        ,mcl.wecom_tag
        ,date(mcl.event_start_dt) as event_start_dt
        ,date(mcl.event_end_dt) as event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code,
        trans.lego_store_code,
        trans.original_store_code,
        trans.date_id,
        trans.crm_member_id,
        trans.distributor_name,
        trans.if_eff_order_tag,
        trans.original_order_id,
        new_member.join_date as  join_time,
        case when trans.crm_member_id IS NOT NULL then '1' else '0' end                                         as is_member,
        CASE WHEN new_member.member_detail_id IS NOT NULL THEN '1' ELSE '0' END                                 AS is_new_member,
        sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)         AS sales,
        sum(case when trans.crm_member_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)    AS mbr_sales,
        sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member.member_detail_id IS NOT NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS new_mbr_sales,
        sum(case when crm_member_id IS NOT NULL AND new_member.member_detail_id IS NULL AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when crm_member_id IS NOT NULL AND new_member.member_detail_id IS NULL AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS existing_mbr_sales,
        -- order_rrp_amt AS sales,
        COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE and crm_member_id is not null THEN crm_member_id ELSE NULL END)                AS ttl_member_shopper,
        SUM(CASE WHEN crm_member_id IS NOT NULL THEN sales_qty ELSE 0 END)                                          AS pieces
        ,SUM(CASE WHEN new_member.member_detail_id IS NOT NULL THEN sales_qty ELSE 0 END)                           AS new_mbr_pieces,
        SUM(CASE WHEN crm_member_id IS NOT NULL AND new_member.member_detail_id IS NULL THEN sales_qty ELSE 0 END) AS existing_mbr_pieces
        -- COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE and trans.crm_member_id IS NOT NULL THEN trans.original_order_id ELSE NULL END)      AS mbr_order_cnt
    FROM edw.f_member_order_detail trans
    inner join  (
        select * 
        from 
            tutorial.marketing_campaign_info_base_table 
        where event_store_type='内场'
        )mcl
        on UPPER(trans.original_store_code) = UPPER(mcl.original_store_code)
        and UPPER(trans.lego_store_code) = UPPER(mcl.lego_store_code)
        and DATE(trans.date_id) >= date(mcl.event_start_dt)
        AND DATE(trans.date_id) <= date(mcl.event_end_dt)
    LEFT JOIN new_member 
     --subeventname对上的一波人
        ON trans.crm_member_id::integer = new_member.member_detail_id::integer
    -- and trans.lego_store_code=new_member.eff_reg_store
        and mcl.sub_event_name=new_member.sub_event_name

    -- and trans.date_id=new_member.join_date
    WHERE is_rrp_sales_type = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

,final as
(select * from sales_shopper
union
select * from sales_shopper_2)

select *
    ,to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date
    ,getdate()                                                   AS dl_load_time
from final
;