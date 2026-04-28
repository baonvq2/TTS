with base as (
    select 
        global_seller_id,
        seller_status,
        gmv_tier_t3,
        prior_30d_gmv_tier_t3,
        gmv_tier_t2,
        prior_30d_gmv_tier_t2,
        seller_group_t3,
        seller_group_t2,
        pay_amt_usd_30d,
        seller_center_pv_30d,
        top_pay_product_lvl1_industry,
        top_sale_product_lvl1_industry,
        main_aov_l30d,
        sub_aov_l30d,
        primary_gmv_type,
        seller_impacted_by_product_deactivation,
        is_bind_tsp,
        is_am_manage,
        exposure_per_product,
        exposure_per_ec_content,
        is_top_video_concentration,
        is_top_live_concentration,
        seller_video_all_30d,
        seller_video_self_30d,
        seller_video_affiliate_30d,
        self_video_ratio_l30d,
        alliance_video_ratio_l30d,
        seller_live_all_30d,
        seller_live_self_30d,
        seller_live_affiliate_30d,
        self_live_ratio_l30d,
        alliance_live_ratio_l30d,
        sellable_product_cnt_l30d,
        product_wstock_more_than_5_l30d,
        oos_rate_l30d,
        stock_per_product,
        total_plan_cnt_l30d,
        open_plan_cnt_l30d,
        target_plan_cnt_l30d,
        avg_open_commission_rate_l30d,
        avg_min_target_commission_rate_l30d,
        avg_max_target_commission_rate_l30d,
        good_comment_cnt_td,
        bad_comment_cnt_td,
        ses_l30d,
        pdp_score_l30d,
        avg_first_cate_price_tier,
        avg_sale_first_cate_price_tier,
        low_price_pct,
        med_high_price_pct,
        daily_sellable_product_l30d,
        daily_searchable_product_l30d,
        daily_recommendable_product_l30d,
        sale_perf_score_l30d,
        late_onset_rate_l30d,
        negative_review_rate_l30d,
        avg_review_star_l30d,
        review_rate_l30d,
        img_review_rate_l30d,
        complaint_rate_l30d,
        roas_l30d,
        video_roas_l30d,
        live_roas_l30d,
        ads_spending,
        video_ads_spending_l30d,
        live_ads_spending_l30d,
        video_product_30d,
        min_video_play_duration_per_uv_l30d,
        avg_video_play_duration_per_uv_l30d,
        avg_video_pv_l30d,
        avg_video_click_l30d,
        avg_video_ctr_l30d,
        video_gpm_l30d,
        live_product_30d,
        min_live_play_duration_per_uv_l30d,
        avg_live_play_duration_per_uv_l30d,
        avg_live_pv_l30d,
        avg_live_click_l30d,
        avg_live_ctr_l30d,
        avg_live_gpm_l30d,
        platform_subsidy_l30d,
        merchant_subsidy_l30d,
        platform_subsidy_rate_l30d,
        merchant_subsidy_rate_l30d,
        max_action_level_l30d,
        violation_ticket_cnt_l30d,
        top_product_gmv_30d,
        top_product_aov_l30d,
        top_product_first_cate_price_tier,
        top_product_sale_first_cate_price_tier,
        top_product_pdp_score,
        top_product_ses_score,
        top_product_stock,
        prior_pay_amt_usd_30d,
        prior_seller_center_pv_30d,
        prior_top_pay_product_lvl1_industry,
        prior_top_sale_product_lvl1_industry,
        prior_main_aov_l30d,
        prior_sub_aov_l30d,
        prior_primary_gmv_type,
        prior_seller_impacted_by_product_deactivation,
        prior_is_bind_tsp,
        prior_is_am_manage,
        prior_exposure_per_product,
        prior_exposure_per_ec_content,
        prior_is_top_video_concentration,
        prior_is_top_live_concentration,
        prior_seller_video_all_30d,
        prior_seller_video_self_30d,
        prior_seller_video_affiliate_30d,
        prior_self_video_ratio_l30d,
        prior_alliance_video_ratio_l30d,
        prior_seller_live_all_30d,
        prior_seller_live_self_30d,
        prior_seller_live_affiliate_30d,
        prior_self_live_ratio_l30d,
        prior_alliance_live_ratio_l30d,
        prior_sellable_product_cnt_l30d,
        prior_product_wstock_more_than_5_l30d,
        prior_oos_rate_l30d,
        prior_stock_per_product,
        prior_total_plan_cnt_l30d,
        prior_open_plan_cnt_l30d,
        prior_target_plan_cnt_l30d,
        prior_avg_open_commission_rate_l30d,
        prior_avg_min_target_commission_rate_l30d,
        prior_avg_max_target_commission_rate_l30d,
        prior_good_comment_cnt_td,
        prior_bad_comment_cnt_td,
        prior_ses_l30d,
        prior_pdp_score_l30d,
        prior_avg_first_cate_price_tier,
        prior_avg_sale_first_cate_price_tier,
        prior_low_price_pct,
        prior_med_high_price_pct,
        prior_daily_sellable_product_l30d,
        prior_daily_searchable_product_l30d,
        prior_daily_recommendable_product_l30d,
        prior_sale_perf_score_l30d,
        prior_late_onset_rate_l30d,
        prior_negative_review_rate_l30d,
        prior_avg_review_star_l30d,
        prior_review_rate_l30d,
        prior_img_review_rate_l30d,
        prior_complaint_rate_l30d,
        prior_roas_l30d,
        prior_video_roas_l30d,
        prior_live_roas_l30d,
        prior_ads_spending,
        prior_video_ads_spending_l30d,
        prior_live_ads_spending_l30d,
        prior_video_product_30d,
        prior_min_video_play_duration_per_uv_l30d,
        prior_avg_video_play_duration_per_uv_l30d,
        prior_avg_video_pv_l30d,
        prior_avg_video_click_l30d,
        prior_avg_video_ctr_l30d,
        prior_video_gpm_l30d,
        prior_live_product_30d,
        prior_min_live_play_duration_per_uv_l30d,
        prior_avg_live_play_duration_per_uv_l30d,
        prior_avg_live_pv_l30d,
        prior_avg_live_click_l30d,
        prior_avg_live_ctr_l30d,
        prior_avg_live_gpm_l30d,
        prior_platform_subsidy_l30d,
        prior_merchant_subsidy_l30d,
        prior_platform_subsidy_rate_l30d,
        prior_merchant_subsidy_rate_l30d,
        prior_max_action_level_l30d,
        prior_violation_ticket_cnt_l30d,
        prior_top_product_gmv_30d,
        prior_top_product_aov_l30d,
        prior_top_product_first_cate_price_tier,
        prior_top_product_sale_first_cate_price_tier,
        prior_top_product_pdp_score,
        prior_top_product_ses_score,
        prior_top_product_stock,
        pay_amt_usd_30d_7d,
        seller_video_self_30d_7d,
        seller_live_self_30d_7d,
        avg_video_ctr_l30d_7d,
        video_gpm_l30d_7d,
        avg_live_ctr_l30d_7d,
        avg_live_gpm_l30d_7d,
        ses_l30d_7d,
        negative_review_rate_l30d_7d,
        pay_amt_usd_30d_14d,
        seller_video_self_30d_14d,
        seller_live_self_30d_14d,
        avg_video_ctr_l30d_14d,
        video_gpm_l30d_14d,
        avg_live_ctr_l30d_14d,
        avg_live_gpm_l30d_14d,
        ses_l30d_14d,
        negative_review_rate_l30d_14d,
        pay_amt_usd_30d_30d,
        seller_video_self_30d_30d,
        seller_live_self_30d_30d,
        avg_video_ctr_l30d_30d,
        video_gpm_l30d_30d,
        avg_live_ctr_l30d_30d,
        avg_live_gpm_l30d_30d,
        ses_l30d_30d,
        negative_review_rate_l30d_30d,
        is_treatment
    FROM
        ecom_da.mission_impact_features_2 --create this table in future
    WHERE
        date = '${date}'
),

treatment_samples AS (
    SELECT  *
    FROM    base
    WHERE   is_treatment = 1
)
INSERT OVERWRITE TABLE ecom_da.mission_impact_features_3 PARTITION(date = '${date}') --gotta create this table
SELECT  *,
        -1 as cohort_id
FROM    treatment_samples
UNION ALL
SELECT  *, 0 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::0'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 1 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::1'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 2 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::2'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 3 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::3'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 4 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::4'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 5 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::5'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 6 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::6'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 7 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::7'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 8 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::8'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 9 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::9'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 10 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::10'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 11 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::11'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 12 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::12'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 13 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::13'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 14 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::14'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 15 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::15'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 16 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::16'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 17 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::17'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 18 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::18'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 19 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::19'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 20 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::20'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 21 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::21'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 22 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::22'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 23 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::23'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 24 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::24'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 25 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::25'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 26 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::26'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 27 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::27'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 28 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::28'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 29 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::29'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 30 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::30'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 31 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::31'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 32 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::32'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 33 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::33'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 34 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::34'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 35 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::35'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 36 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::36'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 37 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::37'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 38 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::38'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 39 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::39'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 40 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::40'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 41 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::41'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 42 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::42'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 43 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::43'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 44 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::44'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 45 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::45'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 46 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::46'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 47 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::47'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 48 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::48'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 49 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::49'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 50 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::50'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 51 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::51'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 52 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::52'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 53 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::53'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 54 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::54'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 55 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::55'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 56 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::56'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 57 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::57'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 58 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::58'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 59 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::59'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 60 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::60'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 61 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::61'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 62 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::62'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 63 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::63'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 64 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::64'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 65 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::65'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 66 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::66'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 67 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::67'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 68 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::68'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 69 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::69'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 70 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::70'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 71 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::71'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 72 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::72'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 73 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::73'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 74 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::74'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 75 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::75'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 76 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::76'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 77 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::77'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 78 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::78'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 79 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::79'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 80 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::80'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 81 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::81'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 82 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::82'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 83 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::83'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 84 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::84'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 85 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::85'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 86 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::86'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 87 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::87'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 88 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::88'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 89 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::89'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 90 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::90'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 91 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::91'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 92 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::92'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 93 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::93'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 94 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::94'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 95 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::95'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 96 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::96'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 97 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::97'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 98 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::98'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))
UNION ALL
SELECT  *, 99 as cohort_id FROM    base WHERE   is_treatment = 0 AND (abs(hash(concat(global_seller_id, ':::99'))) % 1000) * ((SELECT count(1) FROM base) / 1000) < INT (LEAST((SELECT count(1) FROM treatment_samples) * 10,(SELECT count(1) FROM base) * 0.8))

