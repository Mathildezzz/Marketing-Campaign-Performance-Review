
delete tutorial.event_performance_wechat_register;
insert into tutorial.event_performance_wechat_register


-- drop table if exists tutorial.event_performance_wechat_register;
-- create table tutorial.event_performance_wechat_register as 

--注册成功的union记录 base表
with rs as
(
    select
        event_key,
        timestamp with time zone 'epoch' + event_time * interval '1 second' as event_time,
        b.union_id,
        a.gio_id,
        c.wecom_channel_source as latest_wecom_channel_source,
        d.eff_reg_store,
        d.join_time,
        d.member_detail_id
    from stg.gio_event_local a
    left join
        (
            select
                prop_value as union_id,
                gio_id
            from stg.gio_id_user_account_all
            where 1 = 1
            and prop_key = 'id_WMP_unionid'
        ) as b
        on a.gio_id = b.gio_id 
    left join edw.d_dl_sa_external_user_tag as c
        on b.union_id  = c.union_id
    left join
        (
            select
                prop_value as crm_member_id,
                gio_id
            from stg.gio_id_user_account_all
            where 1 = 1
            and prop_key = 'id_CRM_memberid'
        ) as b2
        on a.gio_id = b2.gio_id 
    left join edw.d_member_detail d
        on b2.crm_member_id::int = d.member_detail_id::int
    where 1 = 1
        and a.event_key = 'registerSuccess'
        and (timestamp with time zone 'epoch' + event_time * interval '1 second') < current_date
)

--外场 注册成功的union记录+flag字段
,register_success as
(
    select
        '外场'as event_store_type
        ,mcl.event_name
        ,mcl.sub_event_name
        ,mcl.wecom_tag
        ,mcl.event_start_dt
        ,mcl.event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code
        ,mcl.reg_store_code as event_reg_store_code
        ,rs.union_id
        ,rs.member_detail_id
        ,rs.join_time--注册时间
        ,rs.event_time--扫码时间？
        ,rs.eff_reg_store
        ,rs.latest_wecom_channel_source
        ,case when rs.member_detail_id is not null then 1
            else 0
        end as is_member
        --是否是subevent下的new member
        ,case when date(mcl.event_start_dt) <= date(rs.join_time) and date(rs.join_time) <= date(mcl.event_end_dt)  and UPPER(rs.eff_reg_store) = UPPER(mcl.reg_store_code) then 1
            else 0
        end as is_event_new_member
    from rs
    inner join  
         (
        select * 
        from 
            tutorial.marketing_campaign_info_base_table_v2
        where event_store_type='外场'
        ) mcl
    on UPPER(rs.eff_reg_store) = UPPER(mcl.reg_store_code) --注册店铺
        and DATE(rs.event_time) >= date(mcl.event_start_dt) --注册时间段
        AND DATE(rs.event_time) <= date(mcl.event_end_dt)
        and rs.latest_wecom_channel_source = mcl.wecom_tag--有这个条件即为外场
        
)

-- select * from register_success

----所有活动期间注册会员----
,all_member as (
    SELECT distinct
        mcl.event_name
        ,mcl.sub_event_name
        ,mcl.wecom_tag
        ,mcl.event_start_dt
        ,mcl.event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code
        ,mcl.reg_store_code as event_reg_store_code
        ,md.member_detail_id
        ,DATE(md.join_time) as join_time
        ,md.eff_reg_store
    FROM edw.d_member_detail md
    left join  (
        select *
        from 
            tutorial.marketing_campaign_info_base_table_v2 
        where event_store_type='内场'--只是为了一场活动取一行 内外场的reg_store_code都一样
        ) mcl
        on UPPER(md.eff_reg_store) = UPPER(mcl.reg_store_code) ----ori内场 lego外场 ori包含内外场所有newmember
    where DATE(join_time) >= DATE(mcl.event_start_dt)
        AND DATE(join_time) <= DATE(mcl.event_end_dt)
 )
 
 ---内场--
 --（全场-外场）的unionid差集 + flag字段
,store_type as (
    SELECT 
        '内场'as event_store_type
        ,am.event_name
        ,am.sub_event_name
        ,am.wecom_tag
        ,am.event_start_dt
        ,am.event_end_dt
        ,am.event_lego_store_code
        ,am.event_original_store_code
        ,null as union_id
        ,am.member_detail_id
        ,am.join_time--注册时间
        ,null as event_time--扫码时间？
        ,am.eff_reg_store
        ,null as latest_wecom_channel_source
        
        ,case when am.member_detail_id is not null then 1
            else 0
        end as is_member
        ,case when date(am.event_start_dt) <=date(am.join_time) and date(am.join_time) <= date(am.event_end_dt) and UPPER(am.eff_reg_store) = UPPER(am.event_reg_store_code) then 1
            else 0
        end as is_event_new_member
    FROM all_member am  
    left join register_success rs
        on rs.member_detail_id::int = am.member_detail_id::int
    where rs.member_detail_id is null
)

-- select * from store_type
----全场购买订单的人，分别在内/外场注册的，则分别为内外场的01，例：外场购买，内场注册，为内场的01
--这里拉出分别在内场/外场购买的人 订单 金额
, initial_event_shopper as (
        
    select 
        mcl.event_name
        ,mcl.sub_event_name
        ,mcl.event_store_type
        ,mcl.wecom_tag
        ,mcl.event_start_dt
        ,mcl.event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code
        ,mcl.reg_store_code as event_reg_store_code
        ,case when trans.if_eff_order_tag IS TRUE then trans.crm_member_id else null end as crm_member_id
        ,case when trans.if_eff_order_tag IS TRUE then trans.original_order_id else null end as original_order_id
        ,trans.original_store_code 
        ,trans.lego_store_code 
        ,date(trans.order_paid_time) as order_paid_time
        ,first_paid.initial_order_paid_time
        ,sum(case when trans.sales_qty > 0 then trans.order_rrp_amt else 0 end) - sum(case when trans.sales_qty < 0 then abs(trans.order_rrp_amt) else 0 end)         AS sales
        -- ,ROW_NUMBER() OVER (PARTITION BY trans.crm_member_id ,mcl.sub_event_name ORDER BY trans.order_paid_time ASC) AS shopper_rank  
    from 
         edw.f_member_order_detail trans 
    inner join  (
        select * 
        from 
            tutorial.marketing_campaign_info_base_table_v2 
        -- where event_store_type='内场'
        ) mcl
        on UPPER(trans.original_store_code) = UPPER(mcl.original_store_code)
            and UPPER(trans.lego_store_code) = UPPER(mcl.lego_store_code)
            and date(trans.date_id) >= date(mcl.event_start_dt)
            AND date(trans.date_id) <= date(mcl.event_end_dt)
    left join(
        select distinct
            mcl.sub_event_name
            ,case when trans.if_eff_order_tag IS TRUE then trans.crm_member_id else null end as crm_member_id
            ,date(min(trans.order_paid_time)) as initial_order_paid_time
            --by人，subevent的首单时间（不需要区分内外场去排序,这里不取storetype）
        from 
             edw.f_member_order_detail trans 
        inner join  (
            select * 
            from 
                tutorial.marketing_campaign_info_base_table_v2 
            ) mcl
            on UPPER(trans.original_store_code) = UPPER(mcl.original_store_code)
                and UPPER(trans.lego_store_code) = UPPER(mcl.lego_store_code)
                and date(trans.date_id) >= date(mcl.event_start_dt)
                AND date(trans.date_id) <= date(mcl.event_end_dt)
        where  trans.is_rrp_sales_type = 1
        group by 1,2
        )first_paid 
        on first_paid.sub_event_name=mcl.sub_event_name
            and first_paid.crm_member_id::int=trans.crm_member_id
    where  trans.is_rrp_sales_type = 1
        -- AND  trans.if_eff_order_tag IS TRUE
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    order by sub_event_name,crm_member_id,order_paid_time

)

-- select * from initial_event_shopper where crm_member_id in ( select distinct crm_member_id from initial_event_shopper where order_paid_time <>initial_order_paid_time )order by sub_event_name,crm_member_id,order_paid_time
-- ------------内场 union记录+flag字段+01转化flag---------
,a as (
    select
    -- count(distinct member_detail_id) -- result = 3564, in CDP = 3533
    -- distinct 
        rs.event_store_type
        ,rs.event_name
        ,rs.sub_event_name
        ,rs.wecom_tag
        ,rs.event_start_dt
        ,rs.event_end_dt
        ,rs.event_lego_store_code
        ,rs.event_original_store_code
        ,rs.union_id
        ,rs.member_detail_id
        ,rs.join_time--注册时间
        ,rs.event_time--扫码时间？
        ,rs.eff_reg_store
        
        ,trans.original_order_id
        ,trans.order_paid_time
        ,trans.initial_order_paid_time
        ,trans.sales
        
        ,rs.is_member
        ,rs.is_event_new_member
        ,case when trans.crm_member_id is not null 
            and date(rs.event_start_dt)<= date(trans.order_paid_time) and date(trans.order_paid_time)<=date(rs.event_end_dt) 
        then 1
            else 0
        end as is_0_1_shopper
  
    from 
        store_type rs
    left join  initial_event_shopper trans 
         on rs.member_detail_id::int = trans.crm_member_id::int --memberid不会同时出现在a/b，所以两边的sales不会重复
            and rs.sub_event_name = trans.sub_event_name--只关联到subevent，不区分storetype

)

-- select * from a
-- ------------外场 union记录+flag字段+01转化flag---------
,b as (
    select
    -- count(distinct member_detail_id) -- result = 3564, in CDP = 3533
    -- distinct 
        '外场' as event_store_type
        ,rs.event_name
        ,rs.sub_event_name
        ,rs.wecom_tag
        ,rs.event_start_dt
        ,rs.event_end_dt
        ,rs.event_lego_store_code
        ,rs.event_original_store_code
        ,rs.union_id
        ,rs.member_detail_id
        ,rs.join_time--注册时间
            ,rs.event_time--扫码时间？
        ,rs.eff_reg_store
        
        ,trans.original_order_id
        ,trans.order_paid_time
        ,trans.initial_order_paid_time
        ,trans.sales
        
        ,rs.is_member
        ,rs.is_event_new_member
        ,case when trans.crm_member_id is not null 
        and date(rs.event_start_dt)<= date(trans.order_paid_time) and date(trans.order_paid_time)<=date(rs.event_end_dt) 
        then 1
            else 0
        end as is_0_1_shopper
        
    from 
        register_success rs
    left join  initial_event_shopper trans 
         on rs.member_detail_id::int = trans.crm_member_id::int
        and rs.sub_event_name = trans.sub_event_name
)


 ,final as(
    select * from a
    union
    select * from b
)

select *
    ,to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date
    ,getdate()                                                   AS dl_load_time
from final

;
