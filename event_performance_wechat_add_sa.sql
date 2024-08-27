
delete tutorial.event_performance_wechat_add_sa;
insert into tutorial.event_performance_wechat_add_sa
   -----------5-----扫码加企微--------
   
--扫码base表
 with add_sa as
    (
        -- 添加SA，event_key = 'external_user_add'
        select
            a.event_key, 
            timestamp with time zone 'epoch' + a.event_time * interval '1 second' as event_time, 
            json_extract_path_text(a.attributes, 'store_codes', true) as store_codes,
            json_extract_path_text(a.attributes, 'unionId', true) as unionId ,
            json_extract_path_text(a.attributes, 'external_userId', true) as external_userId,
            json_extract_path_text(a.attributes, 'add_way', true) as add_way, 
            json_extract_path_text(a.attributes, 'code_name', true) as code_name,
            b.crm_member_id,
            c.wecom_channel_source as latest_wecom_channel_source -- CDP中的“最近以此添加企微场景”
        from stg.gio_event_local as a
        left join
            (
                select
                    prop_value as crm_member_id,
                    gio_id
                from stg.gio_id_user_account_all
                where 1 = 1
                and prop_key = 'id_CRM_memberid'
            ) as b
            on a.gio_id = b.gio_id 
        left join edw.d_dl_sa_external_user_tag as c
            on json_extract_path_text(a.attributes, 'unionId', true) = c.union_id 
        where 1 = 1
            and a.event_key = 'external_user_add'  
            and timestamp with time zone 'epoch' + a.event_time * interval '1 second' < current_date
            --  and date(timestamp with time zone 'epoch' + event_time * interval '1 second' ) >'2024-08-19'
    )

-- 全场，用于全场-外场得到内场差集
,all_add as (
    select distinct 
        'all' as event_store_type
        ,mcl.event_name
        ,mcl.sub_event_name
        ,mcl.wecom_tag
        ,date(mcl.event_start_dt) as event_start_dt
        ,date(mcl.event_end_dt) as event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code
        ,sa.unionId
        ,sa.crm_member_id
        ,sa.event_time
        ,sa.store_codes
    from add_sa sa
    inner join  
        (select * 
        from 
            tutorial.marketing_campaign_info_base_table_v2
        where event_store_type='内场') mcl--只是为了抽取一行，内外场的regstorecode都一样
        on UPPER(sa.store_codes) = UPPER(mcl.reg_store_code)
             and DATE(sa.event_time) >= date(mcl.event_start_dt)
            and DATE(sa.event_time) <= date(mcl.event_end_dt)
            -- and sa.latest_wecom_channel_source = mcl.wecom_tag --放开这个条件即为全场
)

--外场
,store_type_1 as (
    select distinct 
        '外场' as event_store_type
        ,mcl.event_name
        ,mcl.sub_event_name
        ,mcl.wecom_tag
        ,date(mcl.event_start_dt) as event_start_dt
        ,date(mcl.event_end_dt) as event_end_dt
        ,mcl.lego_store_code as event_lego_store_code
        ,mcl.original_store_code as event_original_store_code
        ,sa.unionId
        ,sa.crm_member_id
        ,sa.event_time
        ,sa.store_codes
    from add_sa sa
    inner join  (
        select * 
        from 
            tutorial.marketing_campaign_info_base_table_v2 
        where event_store_type='外场'
        )mcl
        on  UPPER(sa.store_codes) = UPPER(mcl.reg_store_code)
            and DATE(sa.event_time) >= date(mcl.event_start_dt)
            and DATE(sa.event_time) <= date(mcl.event_end_dt)
            and sa.latest_wecom_channel_source = mcl.wecom_tag--有这个条件即为外场
    )
    

,store_type_2 as
    (select distinct 
        '内场' as event_store_type
        ,ar.event_name
        ,ar.sub_event_name
        ,ar.wecom_tag
        ,ar.event_start_dt
        ,ar.event_end_dt
        ,ar.event_lego_store_code
        ,ar.event_original_store_code
        ,ar.unionId
        ,ar.crm_member_id
        ,ar.event_time
        ,ar.store_codes
    from all_add ar
    left join store_type_1  b 
        on ar.unionId = b.unionId
        and ar.sub_event_name = b.sub_event_name
    where b.unionId is null
        -- and b.sub_event_name is null
    )
    
 
 ,final as(
    select * from store_type_2
    union
    select * from store_type_1
)

select *
    ,to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date
    ,getdate()                                                   AS dl_load_time
from final

;