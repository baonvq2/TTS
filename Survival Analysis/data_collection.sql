with seller_observation as (
    select  distinct t1.global_seller_id
            , t1.first_t2_tier_date
            , to_date(from_unixtime(unix_timestamp(t1.first_t2_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')) as first_t2_date
            , t1.first_t3_tier_date
            , to_date(from_unixtime(unix_timestamp(t1.first_t3_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')) as first_t3_date
            , least(coalesce(to_date(from_unixtime(unix_timestamp(t1.first_t3_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')), date_add(to_date(from_unixtime(unix_timestamp(t1.first_t2_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')), 119)), date_add(to_date(from_unixtime(unix_timestamp(t1.first_t2_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')), 119)) as final_event_or_censor_date
            , case when t1.first_t3_tier_date is not null 
                    and to_date(from_unixtime(unix_timestamp(t1.first_t3_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')) <= least(coalesce(to_date(from_unixtime(unix_timestamp(t1.first_t3_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')), date_add(to_date(from_unixtime(unix_timestamp(t1.first_t2_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')), 119)), date_add(to_date(from_unixtime(unix_timestamp(t1.first_t2_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')), 119)) 
                        then 1 else 0 end as final_event_status
            , 1 as join_key
    from    ecom.app_country_ops_eu_seller_stats_day t1
    where   1=1
    and     t1.date = '${date}'
    and     t1.register_country_code = 'GB'
    and     t1.first_t2_tier_date is not null
    and     t1.first_t2_tier_date between  '20240101' and '20250228' -- can adjust
    and     (to_date(from_unixtime(unix_timestamp(t1.first_t3_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')) > to_date(from_unixtime(unix_timestamp(t1.first_t2_tier_date, 'yyyyMMdd'), 'yyyy-MM-dd')) or t1.first_t3_tier_date is null)
    -- and     t1.global_seller_id in  (7495823601810639340) -- (7495670443254188948, 7495823601810639340)
)
, numbers as (
    select 0 as n, 1 as join_key
    union all
    select 1 as n, 1 as join_key
    union all
    select 2 as n, 1 as join_key
    union all
    select 3 as n, 1 as join_key
    union all
    select 4 as n, 1 as join_key
    union all
    select 5 as n, 1 as join_key
    union all
    select 6 as n, 1 as join_key
    union all
    select 7 as n, 1 as join_key
    union all
    select 8 as n, 1 as join_key
    union all
    select 9 as n, 1 as join_key
    union all
    select 10 as n, 1 as join_key
    union all
    select 11 as n, 1 as join_key
    union all
    select 12 as n, 1 as join_key
    union all
    select 13 as n, 1 as join_key
    union all
    select 14 as n, 1 as join_key
    union all
    select 15 as n, 1 as join_key
    union all
    select 16 as n, 1 as join_key
    union all
    select 17 as n, 1 as join_key
    union all
    select 18 as n, 1 as join_key
)
, seller_interval as (
    select  t1.global_seller_id
            , t1.first_t2_tier_date
            , t1.first_t2_date
            , t1.first_t3_tier_date
            , t1.first_t3_date
            , t1.final_event_or_censor_date
            , t1.final_event_status
            , t2.n as interval_idx
            , t2.n*7 as start_time_offset_days
            , (t2.n+1)*7 as end_time_offset_days_proposed
            , date_add(t1.first_t2_date, t2.n*7) as interval_actual_start_date
            , date_add(t1.first_t2_date, (t2.n+1)*7) as interval_actual_end_date_proposed
    from    seller_observation t1
    left join
            numbers t2
    on      t1.join_key = t2.join_key
    and     t2.n <= (floor(abs(datediff(t1.first_t2_date, t1.final_event_or_censor_date)))/7+1)
    where   1=1
    and     (t2.n*7) < abs(datediff(t1.first_t2_date, t1.final_event_or_censor_date))
)
, clean_interval as (
    select  distinct t1.global_seller_id
            , t1.first_t2_date
            , t1.first_t3_date
            , t1.final_event_or_censor_date
            , t1.final_event_status
            , t1.start_time_offset_days as start_time
            , least(t1.end_time_offset_days_proposed, abs(datediff(t1.first_t2_date, t1.final_event_or_censor_date))) as stop_time
            , case when t1.final_event_status=1 
                and t1.first_t3_date is not null 
                and t1.first_t3_date>=t1.interval_actual_start_date
                and t1.first_t3_date <= least(t1.interval_actual_end_date_proposed, t1.final_event_or_censor_date)
                then 1 else 0 end as event_occurred
            , date_format(t1.interval_actual_start_date, 'yyyyMMdd') as interval_actual_start_date
            , date_format(least(t1.interval_actual_end_date_proposed, t1.final_event_or_censor_date), 'yyyyMMdd') as interval_metrics_end_date
    from    seller_interval t1
    where   1=1
    and     least(t1.end_time_offset_days_proposed, abs(datediff(t1.first_t2_date, t1.final_event_or_censor_date))) >= start_time_offset_days
)
, product as (
    select  t1.date
            ,t1.global_seller_id
            , count(distinct t1.third_category_id) as third_cate_count
            , count(distinct case when t1.available_quantity>0 then t1.product_id end) as product_w_stock
            , count(distinct case when t1.flash_sale_usd_writeoff_order_amt_1d>0 then t1.product_id end) as product_w_fs
            , avg(t1.hot_sku_min_sale_price) as avg_sale_price
    from    ecom.app_shop_product_indicator_df t1
    inner join (select distinct global_seller_id from seller_observation) t2 on t1.global_seller_id = t2.global_seller_id
    where   1=1
    and     t1.date >= '20240101'
    and     t1.country_code = 'GB'
    and     t1.sale_status = 3
    group by
            t1.date
            , t1.global_seller_id
)
, ads as (
    select  t1.date
            , t1.global_seller_id
            , sum(t1.ad_dollar_cost) as ads_spending
    from    ecom.dm_ad_creative_effect_day t1
    -- inner join (select distinct global_seller_id from seller_observation) t2 on t1.global_seller_id = t2.global_seller_id
    where   1=1
    and     t1.date >= '20240101'
    and     t1.is_ug_advertiser = 0
    and     t1.shop_seller_type_name = 'Local'
    and     t1.ad_type = 'ads manager'
    group by
            t1.date, t1.global_seller_id
)
, video as (
    select  t1.date
            , t1.global_seller_id
            , count(distinct case when t1.create_date = t1.date then t1.item_id end) as seller_video_all
            , count(distinct case when t1.create_date = t1.date and t1.is_seller = 1 then t1.item_id end) as seller_video_self
            , count(distinct case when t1.create_date = t1.date and t1.is_seller = 0 then t1.item_id end) as seller_video_alc
            , count(distinct case when t1.create_date = t1.date and t1.duration <= 15 then t1.item_id end) as seller_video_0_15s
            , count(distinct case when t1.create_date = t1.date and t1.duration <= 60 and t1.duration>15 then t1.item_id end) as seller_video_15_60s
            , count(distinct case when t1.create_date = t1.date and t1.duration > 60 then t1.item_id end) as seller_video_60s

            , count(distinct case when t1.video_product_show_cnt>0 then t1.item_id end) as active_seller_video_all
            , count(distinct case when t1.video_product_show_cnt>0 and t1.is_seller = 1 then t1.item_id end) as active_seller_video_self
            , count(distinct case when t1.video_product_show_cnt>0 and t1.is_seller = 0 then t1.item_id end) as active_seller_video_alc
            , count(distinct case when t1.video_product_show_cnt>0 and t1.duration <= 15 then t1.item_id end) as active_seller_video_0_15s
            , count(distinct case when t1.video_product_show_cnt>0 and t1.duration <= 60 and t1.duration>15 then t1.item_id end) as active_seller_video_15_60s
            , count(distinct case when t1.video_product_show_cnt>0 and t1.duration > 60 then t1.item_id end) as active_seller_video_60s
    from    ecom.dm_video_ecom_product_stats_di t1
    -- inner join (select distinct global_seller_id from seller_observation) t2 on t1.global_seller_id = t2.global_seller_id
    where   1=1
    and     t1.date >= '20240101'
    and     t1.author_operation_country = 'GB'
    group by
            t1.date, t1.global_seller_id
)
, live as (
    select  t1.date
            ,t1.global_seller_id
            , count(distinct case when date_format(from_unixtime(t1.live_start_ts), 'yyyyMMdd')=t1.date then t1.room_id end) as seller_live_all
            , count(distinct case when date_format(from_unixtime(t1.live_start_ts), 'yyyyMMdd')=t1.date and t1.is_seller = 1 then t1.room_id end) as seller_live_self
            , count(distinct case when date_format(from_unixtime(t1.live_start_ts), 'yyyyMMdd')=t1.date and t1.is_seller = 0 then t1.room_id end) as seller_live_alc
            , count(distinct case when date_format(from_unixtime(t1.live_start_ts), 'yyyyMMdd')=t1.date and t1.live_duration_1d<1800 then t1.room_id end) as seller_live_0_30m
            , count(distinct case when date_format(from_unixtime(t1.live_start_ts), 'yyyyMMdd')=t1.date and t1.live_duration_1d>=1800 and t1.live_duration_1d<3600 then t1.room_id end) as seller_live_30_60m
            , count(distinct case when date_format(from_unixtime(t1.live_start_ts), 'yyyyMMdd')=t1.date and t1.live_duration_1d>=3600 and t1.live_duration_1d <7200 then t1.room_id end) as seller_live_60_120m
            , count(distinct case when date_format(from_unixtime(t1.live_start_ts), 'yyyyMMdd')=t1.date and t1.live_duration_1d>=7200 then t1.room_id end) as seller_live_120m
    from    ecom.dm_live_ecom_product_stats_di t1
    -- inner join (select distinct global_seller_id from seller_observation) t2 on t1.global_seller_id = t2.global_seller_id
    where   1=1
    and     t1.date >= '20240101'
    and     t1.author_operation_country = 'GB'
    group by
            t1.date, t1.global_seller_id
)
, subsidy as (
    select   date_format(to_date(from_unixtime(t1.payment_ts)), 'yyyyMMdd') as date_
            , t1.seller_id as global_seller_id
            , sum(t1.subtotal_deduction_platform_amt/cast(t1.pay_mid_price as float)) as platform_subsidy
            , sum(t1.subtotal_deduction_amt/cast(t1.pay_mid_price as float)) as merchant_subsidy
    from    ecom.dm_order_cl_info_df t1
    -- inner join (select distinct global_seller_id from seller_observation) t2 on t1.seller_id = t2.global_seller_id
    where   1=1
    and     t1.date = '${date}'
    and     t1.seller_register_country = 'GB'
    and     date_format(to_date(from_unixtime(t1.payment_ts)), 'yyyyMMdd') is not null
    and     date_format(to_date(from_unixtime(t1.payment_ts)), 'yyyyMMdd') >= '20240101'

    group by date_format(to_date(from_unixtime(t1.payment_ts)), 'yyyyMMdd'), t1.seller_id
)
, plan AS (
    SELECT  t1.begin_bind_plan_date as date_
            , t1.seller_id as global_seller_id
            , count(
                DISTINCT CASE WHEN plan_type = 'public_plan' THEN meta_plan_id
                     ELSE NULL
                END
            ) AS open_plan_cnt
            , count(
                DISTINCT CASE WHEN plan_type = 'target_plan' THEN meta_plan_id
                     ELSE NULL
                END
            ) AS target_plan_cnt
            , avg(
                DISTINCT CASE WHEN plan_type = 'public_plan' THEN plan_commission_rate
                     ELSE NULL
                END
            ) AS open_plan_commission_rate
            , avg(
                DISTINCT CASE WHEN plan_type = 'target_plan' THEN plan_commission_rate
                     ELSE NULL
                END
            ) AS target_plan_commission_rate          
            , count(
                DISTINCT CASE WHEN plan_type = 'public_plan' THEN concat(author_id, product_id)
                     ELSE NULL
                END
            ) AS open_pairs_cnt
            , count(
                DISTINCT CASE WHEN plan_type = 'target_plan' THEN concat(author_id, product_id)
                     ELSE NULL
                END
            ) AS target_pairs_cnt
    from    ecom.dm_alliance_plan_author_stats_df t1
    -- inner join (select distinct global_seller_id from seller_observation) t2 on t1.global_seller_id = t2.global_seller_id 
    where   t1.date = '${date}'
    and     t1.product_available_country = 'GB'
    group by
            begin_bind_plan_date, t1.seller_id
)
, sample as (
    select  distinct t1.date
            , 'Direct' as sample_type
            , apply_id
            , group_type
            , null as sample_order_id
            , status
            , seller_id
            , seller_approve_date
            , payment_gmv_usd as usd_gmv_1d
            , null as voucher_deduction_amt
    from    ecom.app_alc_free_sample_apply_df t1
    where   1=1
    and     t1.date >= '20240101'
    and     t1.create_date >= '20240401'
    and     t1.product_avaliable_country = 'GB'
    -- and     t1.global_seller_type = 'Local'

    union all

    select  distinct t1.date
            , 'Voucher' as sample_type
            , coupon_id as apply_id
            , null as group_type
            , sample_order_id
            , case
                when sample_order_status=1 then 'Claimed'
                when sample_order_status=2 then 'Used'
                when sample_order_status=3 then 'Pre Transit'
                when sample_order_status=4 then 'Delivered'
                when sample_order_status=5 then 'Timeout'
                when sample_order_status=6 then 'Closed'
                when sample_order_status=7 then 'Expired'
                when sample_order_status=9 then 'In Transit'
                else sample_order_status
            end as status
            , shop_id as seller_id
            , null as seller_approve_date
            , usd_payment_gmv_1d as usd_gmv_1d
            , voucher_deduction_amt
    from    ecom.app_author_sample_order_records_day t1
    where   1=1
    and     t1.date >= '20240101'
    and     t1.coupon_create_date >= '20240101'
    and     t1.product_avaliable_country = 'GB'
)
, sample_agg as (
    select  t1.date
            , t1.seller_id as global_seller_id
            , count(distinct case when (seller_approve_date is not null or status in ('Delivered','Completed','RTS','Shipped','Pre Transit','In Transit','ContentPending','Used')) 
                    then concat(coalesce(sample_order_id, apply_id), date) end) / count(distinct date) as daily_approved_sample
            -- , count(distinct case when (t1.date between  date_format(date_add(to_date(from_unixtime(unix_timestamp(first_t3_date, 'yyyyMMdd'), 'yyyy-MM-dd')), -29), 'yyyyMMdd') and date_format(to_date(from_unixtime(unix_timestamp(first_t3_date, 'yyyyMMdd'), 'yyyy-MM-dd')), 'yyyyMMdd')) and (seller_approve_date is not null or status in ('Delivered','Completed','RTS','Shipped','Pre Transit','In Transit','ContentPending','Used')) 
            --         and (case when group_type = 9 then 'refundable' when `sample_type` = 'Direct' then 'free sample' when `sample_type` = 'Voucher' then 'sample voucher' end) = 'free sample'
            --         then concat(coalesce(sample_order_id, apply_id), date) end) / count(distinct date) as daily_approved_free_sample
            , count(distinct case when --(seller_approve_date is not null or status in ('Delivered','Completed','RTS','Shipped','Pre Transit','In Transit','ContentPending','Used')) and
                    (case when group_type = 9 then 'refundable' when `sample_type` = 'Direct' then 'free sample' when `sample_type` = 'Voucher' then 'sample voucher' end) = 'refundable'
                    then concat(coalesce(sample_order_id, apply_id), date) end) / count(distinct date) as daily_bnrl_sample
    from    sample t1
    -- inner join (select distinct global_seller_id from seller_observation) t2 on t1.seller_id = t2.global_seller_id
    group by
            t1.date
            , t1.seller_id
)
, ses AS (
    SELECT  date_format(adj_biz_date_15_30, 'yyyyMMdd') as date_
            , t1.global_seller_id,
            SUM(late_dispatch_numerator) / SUM(late_dispatch_denominator) AS ldr,
            SUM(defect_order_cnt_td) / SUM(un_buyer_order_cnt_td) AS dfo,
            SUM(modify_responsibility_cancel_sku_order_cnt) / SUM(stocking_order_cnt_td) AS sfcr,
            SUM(negative_review_order_cnt_td) / SUM(review_order_cnt_td) AS nrr,
            SUM(negative_review_order_cnt_td) / SUM(un_cancelled_order_cnt_td) AS onrr
    FROM    ecom.app_gvn_shop_prd_info_day_2_eu2va_fordes t1
    -- inner join (select distinct global_seller_id from seller_observation) t2 on t1.global_seller_id = t2.global_seller_id
    WHERE   date = '${date}'
    GROUP BY
            adj_biz_date_15_30, t1.global_seller_id
)
, fan as (
    select  distinct t1.global_seller_id
            , t1.date
            , t1.fans_cnt_td
    from    ecom.app_country_ops_eu_seller_stats_day t1
    where   1=1
    and     t1.date >= '20240101'
    and     t1.register_country_code = 'GB'
)
select  t1.global_seller_id
        , t1.first_t2_date
        , t1.first_t3_date
        , t1.start_time
        , t1.stop_time
        , t1.interval_actual_start_date
        , t1.interval_metrics_end_date
        , t1.event_occurred
        --product
        , max(case when t2.date=t1.interval_metrics_end_date then t2.third_cate_count end) as third_cate_count
        , sum(case when t2.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t2.product_w_stock end)/count(distinct case when t2.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t2.date end) as product_w_stock
        , sum(case when t2.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t2.product_w_fs end)/count(distinct case when t2.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t2.date end) as product_w_fs
        , avg(case when t2.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t2.avg_sale_price end) as avg_sale_price
        --video
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.seller_video_all end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as seller_video_all
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.seller_video_self end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as seller_video_self
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.seller_video_alc end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as seller_video_alc
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.seller_video_0_15s end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as seller_video_0_15s
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.seller_video_15_60s end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as seller_video_15_60s
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.seller_video_60s end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as seller_video_60s
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.active_seller_video_all end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as active_seller_video_all
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.active_seller_video_self end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as active_seller_video_self
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.active_seller_video_alc end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as active_seller_video_alc
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.active_seller_video_0_15s end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as active_seller_video_0_15s
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.active_seller_video_15_60s end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as active_seller_video_15_60s
        , sum(case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.active_seller_video_60s end)/count(distinct case when t3.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t3.date end) as active_seller_video_60s
        --live
        , sum(case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.seller_live_all end)/count(distinct case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.date end) as seller_live_all
        , sum(case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.seller_live_self end)/count(distinct case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.date end) as seller_live_self
        , sum(case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.seller_live_alc end)/count(distinct case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.date end) as seller_live_alc
        , sum(case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.seller_live_0_30m end)/count(distinct case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.date end) as seller_live_0_30m
        , sum(case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.seller_live_30_60m end)/count(distinct case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.date end) as seller_live_30_60m
        , sum(case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.seller_live_60_120m end)/count(distinct case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.date end) as seller_live_60_120m
        , sum(case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.seller_live_120m end)/count(distinct case when t4.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t4.date end) as seller_live_120m
        --subsidy
        , sum(case when t5.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t5.merchant_subsidy end) as merchant_subsidy
        , sum(case when t5.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t5.platform_subsidy end) as platform_subsidy
        -- --ads
        , sum(case when t6.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t6.ads_spending end) as ads_spending
        --plan
        , sum(case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.open_plan_cnt end)/count(distinct case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.date_ end) as open_plan_cnt
        , sum(case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.target_plan_cnt end)/count(distinct case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.date_ end) as target_plan_cnt
        , sum(case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.open_pairs_cnt end)/count(distinct case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.date_ end) as open_pairs_cnt
        , sum(case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.target_pairs_cnt end)/count(distinct case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.date_ end) as target_pairs_cnt
        , avg(case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.open_plan_commission_rate end) as open_plan_commission_rate
        , avg(case when t7.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t7.target_plan_commission_rate end) as target_plan_commission_rate
        --sample
        , sum(case when t8.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t8.daily_approved_sample end)/count(distinct case when t8.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t8.date end) as daily_approved_sample
        , sum(case when t8.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t8.daily_bnrl_sample end)/count(distinct case when t8.date between t1.interval_actual_start_date and t1.interval_metrics_end_date then t8.date end) as daily_bnrl_sample
        ses
        , avg(case when t9.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t9.ldr end) as ldr
        , avg(case when t9.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t9.dfo end) as dfo
        , avg(case when t9.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t9.sfcr end) as sfcr
        , avg(case when t9.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t9.nrr end) as nrr
        , avg(case when t9.date_ between t1.interval_actual_start_date and t1.interval_metrics_end_date then t9.onrr end) as onrr
        -- fan
        , max(case when t10.date=t1.interval_metrics_end_date then t10.fans_cnt_td end) as fans_cnt_td
from    clean_interval t1
left join product t2 on t1.global_seller_id = t2.global_seller_id
left join video t3 on t1.global_seller_id = t3.global_seller_id
left join live t4 on t1.global_seller_id = t4.global_seller_id
left join subsidy t5 on t1.global_seller_id = t5.global_seller_id
left join ads t6 on t1.global_seller_id = t6.global_seller_id
left join plan t7 on t1.global_seller_id = t7.global_seller_id
left join sample_agg t8 on t1.global_seller_id = t8.global_seller_id
left join ses t9 on t1.global_seller_id = t9.global_seller_id
left join fan t10 on t1.global_seller_id = t10.global_seller_id
group by
        t1.global_seller_id
        , t1.first_t2_date
        , t1.first_t3_date
        , t1.start_time
        , t1.stop_time
        , t1.interval_actual_start_date
        , t1.interval_metrics_end_date
        , t1.event_occurred
order by t1.global_seller_id, t1.start_time
