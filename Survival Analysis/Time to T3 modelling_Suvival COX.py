#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import json
import requests
from datetime import date, datetime
from datetime import timedelta
from pyspark.sql import functions as F
from scipy import stats
import statsmodels.api as sm
import statsmodels

from sklearn.cluster import KMeans

import seaborn as sns
from matplotlib import pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
from collections import defaultdict, Counter

import warnings
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from lifelines import CoxTimeVaryingFitter
# from scipy.stats import proportions_ztest


# In[2]:


df = pd.read_excel('archived-data/20250626_time_to_t3_survival_v2.xlsx', engine='openpyxl')
df.head(20)


# In[81]:


df_clean = df.copy()
# df_clean = df_clean[df_clean['is_correct_t3']==1]
df_clean.shape


# In[82]:


industry = pd.read_csv('archived-data/20250628_time_to_t3_industry.csv')
industry


# In[83]:


df_clean=df_clean.merge(industry, how='left')
df_clean


# In[84]:


#check null
df_clean.isnull().sum()/len(df_clean)


# In[85]:


df_clean.industry.unique()


# In[86]:


#fill na
df_clean = df_clean[['global_seller_id', 'start_time', 'stop_time', 'event_occurred', 'third_cate_count', 
        'product_w_stock', 'product_w_fs', 'avg_sale_price', 'seller_video_all',
       'seller_video_self', 'seller_video_alc', 'seller_video_0_15s',
       'seller_video_15_60s', 'seller_video_60s', 'active_seller_video_all',
       'active_seller_video_self', 'active_seller_video_alc',
       'active_seller_video_0_15s', 'active_seller_video_15_60s',
       'active_seller_video_60s', 'seller_live_all', 'seller_live_self',
       'seller_live_alc', 'seller_live_0_30m', 'seller_live_30_60m',
       'seller_live_60_120m', 'seller_live_120m', 'merchant_subsidy',
       'platform_subsidy', 'ads_spending', 'open_plan_cnt', 'target_plan_cnt',
       'open_pairs_cnt', 'target_pairs_cnt', 'open_plan_commission_rate',
       'target_plan_commission_rate', 'daily_approved_sample',
       'daily_bnrl_sample', 'ldr', 'dfo', 'sfcr', 'nrr', 'onrr', 'industry']].fillna(0)
#recheck na
df_clean.isnull().sum()/len(df_clean)


# In[87]:


#train data
# df_clean=df_clean[df_clean.industry=='Electronics']
feature = ['third_cate_count', 'product_w_stock', 'product_w_fs', 'avg_sale_price',
       'seller_video_self', 'seller_video_alc', 
    #    'seller_video_0_15s', 'seller_video_15_60s', 'seller_video_60s',
       'active_seller_video_self', 'active_seller_video_alc',
    #    'active_seller_video_0_15s', 'active_seller_video_15_60s', 'active_seller_video_60s', 
       'seller_live_self', 'seller_live_alc', 
    #    'seller_live_0_30m', 'seller_live_30_60m', 'seller_live_60_120m', 'seller_live_120m', 
       'merchant_subsidy', 'platform_subsidy', 'ads_spending', 'open_plan_cnt', 'target_plan_cnt',
       'open_pairs_cnt', 'target_pairs_cnt', 'open_plan_commission_rate',
       'target_plan_commission_rate', 'daily_approved_sample',
       'daily_bnrl_sample', 'ldr', 'dfo', 'nrr', 'onrr']
target = ['event_occurred']
step = ['start_time', 'stop_time']
id_ = ['global_seller_id']
train_cols = id_ + step + target + feature
df_train = df_clean[train_cols]
df_train_original_unscaled = df_clean.copy()

#Scaler
scaler = StandardScaler()
df_train[feature] = scaler.fit_transform(df_train[feature])

#initialize cox
ctv = CoxTimeVaryingFitter(penalizer=0.1)
ctv.fit(df_train,
        id_col='global_seller_id',
        event_col='event_occurred',
        start_col='start_time',
        stop_col='stop_time',
#         max_steps = 200,
        show_progress = True)
print("\n--- Cox Time-Varying Model Summary ---")
# FIX: Just print ctv.summary directly
print(ctv.summary)


# In[88]:


result_df = ctv.summary
result_df[result_df['p']<0.05].sort_values('exp(coef)', ascending=False)


# In[11]:


successful_uplevelers = df[df['is_correct_t3']==1][['global_seller_id', 'first_t2_date', 'first_t3_date', 'start_time', 'stop_time', 'event_occurred', 'third_cate_count', 
        'product_w_stock', 'product_w_fs', 'avg_sale_price', 'seller_video_all',
       'seller_video_self', 'seller_video_alc', 'seller_video_0_15s',
       'seller_video_15_60s', 'seller_video_60s', 'active_seller_video_all',
       'active_seller_video_self', 'active_seller_video_alc',
       'active_seller_video_0_15s', 'active_seller_video_15_60s',
       'active_seller_video_60s', 'seller_live_all', 'seller_live_self',
       'seller_live_alc', 'seller_live_0_30m', 'seller_live_30_60m',
       'seller_live_60_120m', 'seller_live_120m', 'merchant_subsidy',
       'platform_subsidy', 'ads_spending', 'open_plan_cnt', 'target_plan_cnt',
       'open_pairs_cnt', 'target_pairs_cnt', 'open_plan_commission_rate',
       'target_plan_commission_rate', 'daily_approved_sample',
       'daily_bnrl_sample', 'ldr', 'dfo', 'sfcr', 'nrr', 'onrr']].fillna(0)
successful_uplevelers = successful_uplevelers[(successful_uplevelers['first_t3_date'].notna())].copy()
successful_uplevelers_sorted = successful_uplevelers.sort_values(by=['global_seller_id', 'start_time'], ascending=False)
latest_two_intervals = successful_uplevelers_sorted.groupby('global_seller_id').head(2)
latest_two_intervals


# In[12]:


successful_uplevelers = df[df['is_correct_t3']==1].copy()
successful_uplevelers = successful_uplevelers[(successful_uplevelers['first_t3_date'].notna())]
successful_uplevelers = successful_uplevelers[['global_seller_id', 'first_t2_date', 'first_t3_date', 'start_time', 'stop_time', 'event_occurred', 'third_cate_count', 
        'product_w_stock', 'product_w_fs', 'avg_sale_price', 'seller_video_all',
       'seller_video_self', 'seller_video_alc', 'seller_video_0_15s',
       'seller_video_15_60s', 'seller_video_60s', 'active_seller_video_all',
       'active_seller_video_self', 'active_seller_video_alc',
       'active_seller_video_0_15s', 'active_seller_video_15_60s',
       'active_seller_video_60s', 'seller_live_all', 'seller_live_self',
       'seller_live_alc', 'seller_live_0_30m', 'seller_live_30_60m',
       'seller_live_60_120m', 'seller_live_120m', 'merchant_subsidy',
       'platform_subsidy', 'ads_spending', 'open_plan_cnt', 'target_plan_cnt',
       'open_pairs_cnt', 'target_pairs_cnt', 'open_plan_commission_rate',
       'target_plan_commission_rate', 'daily_approved_sample',
       'daily_bnrl_sample', 'ldr', 'dfo', 'sfcr', 'nrr', 'onrr']].fillna(0).copy()
#successful_uplevelers = successful_uplevelers[(successful_uplevelers['event_occurred'] == 1) & (successful_uplevelers['first_t3_date'].notna())].copy()
successful_uplevelers['first_t2_date'] = pd.to_datetime(successful_uplevelers['first_t2_date'])
successful_uplevelers['first_t3_date'] = pd.to_datetime(successful_uplevelers['first_t3_date'])
successful_uplevelers = successful_uplevelers.sort_values(by=['global_seller_id', 'start_time'], ascending=False)
successful_uplevelers = successful_uplevelers.groupby('global_seller_id').head(3)
successful_uplevelers['time_to_t3_actual_days'] = (successful_uplevelers['first_t3_date'] - successful_uplevelers['first_t2_date']).dt.days

def get_uplevel_speed_category(days):
    if days < 14:
        return '1. Fast (<2 weeks)'
    elif days < 30:
        return '2. Medium (2-4 weeks)'
#     elif days <= 60:
#         return '3. Medium (1-2 months)'
    else: # Should ideally not be hit due to filter above, but as a safe fallback
        return '3. Slow (>4 weeks)' 
successful_uplevelers['uplevel_speed_category'] = successful_uplevelers['time_to_t3_actual_days'].apply(get_uplevel_speed_category)
print(f"Number of successful uplevelers for descriptive analysis: {len(successful_uplevelers)}")
print("Uplevel Speed Category Distribution:\n", successful_uplevelers.groupby('uplevel_speed_category')['global_seller_id'].nunique())
successful_uplevelers


# In[13]:


descriptive_averages = successful_uplevelers[successful_uplevelers['uplevel_speed_category']!='4. Other'].groupby('uplevel_speed_category')[feature].mean().T
descriptive_averages.index.name = 'feature'
descriptive_averages.columns.name = 'Uplevel Speed Category'
descriptive_averages


# In[14]:


ctv_summary_df = result_df[result_df['p']<0.05].sort_values('exp(coef)', ascending=False)
ctv_summary_df['Hazard_Increase_Percent'] = (ctv_summary_df['exp(coef)'] - 1) * 100
combined_results = ctv_summary_df.merge(descriptive_averages, left_index=True, right_index=True, how='inner')
display_cols = ['exp(coef)', 'Hazard_Increase_Percent'] + list(descriptive_averages.columns)
combined_results[display_cols].sort_values(by='exp(coef)', ascending=False)


# In[15]:


result_df[result_df['p']<0.05].sort_values('exp(coef)', ascending=False).reset_index()['covariate'].unique()


# In[64]:


df_train_original_unscaled[['seller_live_alc', 'seller_video_alc', 'ads_spending',
       'platform_subsidy', 'seller_live_self', 'merchant_subsidy',
       'active_seller_video_alc', 'product_w_fs',
       'active_seller_video_self', 'target_plan_commission_rate']].describe().transpose()


# In[10]:


df_train['global_seller_id'].nunique()


# In[13]:


df_train[df_train.event_occurred==1]['global_seller_id'].nunique()


# In[24]:


df_train[df_train.event_occurred==1]['global_seller_id'].nunique()/df_train['global_seller_id'].nunique()


# In[36]:


df_clean[(df_clean.global_seller_id==7493991085692126350) | (df_clean.global_seller_id==7493992954860275770)].head(50)


# 

# In[75]:


# --- 3. Plotting Survival Curves (Manual Partial Effects is the ONLY way for CtvFitter) ---

print("\nPlotting predicted survival curves using manual calculation, as 'plot_partial_effects_on_outcome' is not available for CoxTimeVaryingFitter.")

# Get the median values for all covariates, to use as a baseline for hypothetical sellers
# Exclude id, start/stop/event columns for calculating medians
covariates_for_median = df_train.drop(columns=['global_seller_id', 'start_time', 'stop_time', 'event_occurred'])
median_covariates = covariates_for_median.median()

# Example 1: Effect of Avg_Video_Watch_Time_Rolling30D
m = 'product_w_fs'
# Let's plot for a low, median, and high value of Avg_Video_Watch_Time_Rolling30D
m_low = df_train[m].quantile(0.25)
m_median = df_train[m].median()
m_high = df_train[m].quantile(0.75)

plt.figure(figsize=(10, 6))

scenarios = {
    f'Avg Watch Time: {m_low:.0f}s (Low)': {'Avg_Video_Watch_Time_Rolling30D': m_low},
    f'Avg Watch Time: {m_median:.0f}s (Median)': {'Avg_Video_Watch_Time_Rolling30D': m_median},
    f'Avg Watch Time: {m_high:.0f}s (High)': {'Avg_Video_Watch_Time_Rolling30D': m_high},
}

colors = ['red', 'blue', 'green']
line_styles = ['--', '-', ':'] 

for i, (label, override_values) in enumerate(scenarios.items()):
    # Start with the median values for all covariates
    hypothetical_df = median_covariates.to_frame().T
    
    # Override the specific covariate for this scenario
    for key, value in override_values.items():
        hypothetical_df[key] = value
    
    # Correct manual prediction using baseline_survival_ and predict_log_partial_hazard
    log_partial_hazard = ctv.predict_log_partial_hazard(hypothetical_df).iloc[0] # Get the single value
    
    # S(t|X) = S0(t)^exp(log_partial_hazard)
    predicted_sf = ctv.baseline_survival_ ** np.exp(log_partial_hazard)
    
    plt.plot(predicted_sf.index, predicted_sf, label=label, color=colors[i], linestyle=line_styles[i])

plt.title('Predicted Survival Curves by Avg Video Watch Time (Rolling 30D)')
plt.xlabel('Time in T2 (Days)')
plt.ylabel('Probability of NOT Upleveling to T3')
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(title='Scenario')
plt.tight_layout()
plt.show()


# In[54]:


np.arange(3)


# In[67]:


# 1. Get the summary as a DataFrame
summary_df = ctv.summary

# 2. Filter for statistically significant features with positive impact (HR > 1)
significant_positive_impact = summary_df[
    (summary_df['p'] < 0.05) & 
    (summary_df['exp(coef)'] > 1)
].sort_values(by='exp(coef)', ascending=False)

# 3. Select top N features for plotting
num_features_to_plot = 10 # Adjust as desired
top_features_for_plot = significant_positive_impact.index.tolist()[:num_features_to_plot]

if not top_features_for_plot:
    print("\nNo statistically significant positive impact features found to plot.")
else:
    print(f"\nPlotting for top {len(top_features_for_plot)} features:")
    
    # Get the median values for all covariates from the SCALED data for the baseline profile
    baseline_profile_df_scaled = df_train[feature].median().to_frame().T 
    
    # Define the range of the time axis for predictions
    # Use a range similar to your stop_time max, or slightly beyond
    prediction_times = np.arange(0, df_train['stop_time'].max() + 1, 1) # Predict for each day

    for feat_name in top_features_for_plot:
        # Only proceed if the feature was actually scaled (is numerical and not binary/low-nunique)
        if feat_name not in feature:
            print(f"Skipping plotting '{feat_name}' as it's not a scaled numerical feature.")
            continue
        
        # Get the original (unscaled) data for this specific feature
        original_feature_data = df_train_original_unscaled[feat_name].dropna().values 
        # Define a range of original values to test for the chosen feature
        # E.g., from min to max, or 5th to 95th percentile, with 10 steps
        num_points_in_range = 500 # Number of points to sample across the feature's range
        
        # Use quantiles for a robust range
        val_min_orig = np.percentile(original_feature_data, 1) 
        val_max_orig = np.percentile(original_feature_data, 99)
        
        std = np.std(original_feature_data)
        range_ = val_max_orig-val_min_orig
        num = int(range_ / std)
#         print(std)
#         print(range_)
#         print(num)
        
        # Handle cases where min/max are too close or equal (e.g., if it's almost a constant despite nunique > 2)
        if val_max_orig <= val_min_orig:
            val_max_orig = val_min_orig + 0.1 # Add a small delta if range is zero
            if val_max_orig == val_min_orig + 0.1: # if still no range (e.g. constant value)
                print(f"Skipping '{feat_name}' plot due to effectively constant values after percentile check.")
                continue
        original_values_to_plot = np.linspace(val_min_orig, val_max_orig, num_points_in_range)
        
        predicted_median_times = []
        
        for original_val in original_values_to_plot:
            # 1. Scale the current original value
            
            # scaled_val = scaler.transform(np.array([[original_val]]))[:, 0].item() 
            #    Initialize it with zeros (or medians of original unscaled data if you prefer)
            temp_df_for_transform = pd.DataFrame(0.0, index=[0], columns=feature)
            
            # 2. Place the current `original_val` into the correct column of this temporary DataFrame.
            temp_df_for_transform[feat_name] = original_val
            
            # 3. Now, transform this *full-width* temporary DataFrame.
            #    This result is a 2D NumPy array with the scaled values.
            scaled_row_values = scaler.transform(temp_df_for_transform)
            
            # 4. Extract the single scaled value for the feature we just varied.
            #    We need its index within `numerical_features_to_scale`.
            idx_in_scaled_array = feature.index(feat_name)
            scaled_val = scaled_row_values[0, idx_in_scaled_array]
            
            # 2. Create hypothetical profile with this scaled value
            hypothetical_df_scaled = baseline_profile_df_scaled.copy()
            hypothetical_df_scaled[feat_name] = scaled_val
            
            # 3. Calculate log partial hazard
            log_partial_hazard = ctv.predict_log_partial_hazard(hypothetical_df_scaled).iloc[0]
            
            # 4. Calculate predicted survival function
            # Use prediction_times for the time index to ensure consistent output for all curves
            predicted_sf_data_to_series = (
                ctv.baseline_survival_.reindex(prediction_times, method='pad') ** np.exp(log_partial_hazard)
            ).values.flatten() # <--- This is the key change

            predicted_sf_series = pd.Series(
                predicted_sf_data_to_series,
                index=prediction_times
            )
                        
            # 5. Find the median survival time (time when survival probability drops below 0.5)
            median_time = predicted_sf_series[predicted_sf_series <= 0.95].index.min()
            
            # Handle cases where median might not be reached within the max observation period
            if pd.isna(median_time):
                median_time = prediction_times.max() # Or np.nan, or a large sentinel value, depending on how you want to represent "never reached"
                
            predicted_median_times.append(median_time)
        
        # Plotting
        plt.figure(figsize=(10, 6))
        plt.plot(original_values_to_plot, predicted_median_times, linestyle='-', color='blue')
        
        # Customize plot for clarity
        plt.title(f'Predicted Time to T3 vs. {feat_name}')
        plt.xlabel(f'{feat_name} (Original Values)')
        plt.ylabel('Predicted Time to T3 (Days)')
#         plt.ylim((20, 40))
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()
        
#         std = np.std(original_values_to_plot)
        idx_start = int(len(original_values_to_plot) * 0.1)
        idx_end = int(len(original_values_to_plot) * 0.5)

#         range_ = max(original_values_to_plot)-min(original_values_to_plot)
#         num = int(range_ / std)
        percent_change_y = (predicted_median_times[idx_end] - predicted_median_times[idx_start])/predicted_median_times[idx_start]*100
        print(original_values_to_plot[idx_start], original_values_to_plot[idx_end])
#         print(predicted_median_times[num-1], predicted_median_times[0])

#         percent_change_y = []
#         print(original_values_to_plot)
#         print(predicted_median_times)
#         for i in np.arange(num-1):
#             x_start = original_values_to_plot[i]
#             y_start = predicted_median_times[i]
#             x_end = original_values_to_plot[i+1]
#             y_end = predicted_median_times[i+1]
#             delta_y = y_end - y_start
#             percent_change_y.append((delta_y / y_start) * 100)
#             print(y_start, y_end, (delta_y / y_start) * 100)
        
#         x_start = original_values_to_plot[idx_start]
#         y_start = predicted_median_times[idx_start]
        

#         x_end = original_values_to_plot[idx_end]
#         y_end = predicted_median_times[idx_end]
        
#         delta_x = x_end - x_start
#         delta_y = y_end - y_start
        
#         percent_increase_x = ((x_end - x_start) / x_start) * 100
#         percent_change_y = (delta_y / y_start) * 100
        print(percent_change_y)
        print(f"Summary for {feat_name}: A xx% increase in {feat_name} can shorten {np.mean(percent_change_y):.2f}% time to T3.")
print("\n--- Plots of Covariate Change vs. Time to T3 complete ---")


# In[14]:


max(original_values_to_plot)-min(original_values_to_plot)


# In[12]:


# --- Plotting Covariate Value vs. Quantile Time to T3 & Calculating 1-Std Dev Impact ---

# Define the survival probability threshold for the quantile you want to show
# 0.95 means time for 5% uplevel; 0.90 means time for 10% uplevel etc.
survival_threshold = 0.95 # As per your code in the prompt

summary_df = ctv.summary
significant_positive_impact = summary_df[
    (summary_df['p'] < 0.05) & 
    (summary_df['exp(coef)'] > 1)
].sort_values(by='exp(coef)', ascending=False)

num_features_to_plot = 10 
top_features_for_plot = significant_positive_impact.index.tolist()[:num_features_to_plot]

# Define an extended prediction time range to better capture quantiles, even if > 60 days
prediction_times_range_for_quantile = np.arange(0, df_train['stop_time'].max() + 30, 1) # Extend by 30 days for better quantile capture

# Prepare a list to store the 1-Std Dev impact summary
one_std_impact_summary = []

# --- Helper function to get predicted quantile time for a given original value ---
# This function is crucial and needs to be defined before the loop that calls it.
def get_predicted_quantile_time(original_val_input, feat_name_input, 
                                baseline_profile_scaled, scaler_obj, 
                                numerical_feats_list, ctv_fitter, pred_times_range, threshold):
    # 1. Scale the current original value using the fitted scaler
    # Create a temporary full-width DataFrame for scaler.transform
    temp_df_for_transform = pd.DataFrame(0.0, index=[0], columns=numerical_feats_list) # Use numerical_feats_list as columns
    temp_df_for_transform[feat_name_input] = original_val_input
    
    scaled_row_values = scaler_obj.transform(temp_df_for_transform)
    idx_in_scaled_array = numerical_feats_list.index(feat_name_input)
    scaled_val = scaled_row_values[0, idx_in_scaled_array]
    
    # 2. Create hypothetical profile with this scaled value
    hypothetical_df_scaled = baseline_profile_scaled.copy()
    hypothetical_df_scaled[feat_name_input] = scaled_val
    
    # 3. Calculate log partial hazard
    log_partial_hazard = ctv_fitter.predict_log_partial_hazard(hypothetical_df_scaled).iloc[0]
    
    # 4. Calculate predicted survival function
    predicted_sf_data_to_series = (
        ctv_fitter.baseline_survival_.reindex(pred_times_range, method='pad') ** np.exp(log_partial_hazard)
    ).values.flatten() 
    predicted_sf_series = pd.Series(predicted_sf_data_to_series, index=pred_times_range)
    
    # 5. Find the quantile time
    quantile_time = predicted_sf_series[predicted_sf_series <= threshold].index.min()
    if pd.isna(quantile_time):
        quantile_time = pred_times_range.max() # If quantile not reached, cap at max time
    return quantile_time
# --- End of Helper function ---


if not top_features_for_plot:
    print("\nNo statistically significant positive impact features found to plot.")
else:
    print(f"\nPlotting for top {len(top_features_for_plot)} features:")
    
    # Get the median values for ALL features from the SCALED data for the baseline profile
    baseline_profile_df_scaled = df_train[feature].median().to_frame().T 

    for feat_name in top_features_for_plot:
        # Only proceed if the feature was actually scaled (is numerical and not binary/low-nunique)


        original_feature_data = df_train_original_unscaled[feat_name].dropna().values 
        
        # --- Calculate 1-Standard Deviation Impact for this feature ---
        original_mean = df_train_original_unscaled[feat_name].mean()
        original_std = df_train_original_unscaled[feat_name].std()

        # Define the two points for calculation: original mean and original mean + 1 std dev
        point_baseline_orig_val = original_mean
        point_plus_1_std_orig_val = original_mean + original_std

        # Get times at baseline (mean) and +1 std dev using the helper function
        time_at_baseline_mean = get_predicted_quantile_time(
            point_baseline_orig_val, feat_name, baseline_profile_df_scaled, scaler, 
            feature, ctv, prediction_times_range_for_quantile, survival_threshold)
        
        time_at_plus_1_std = get_predicted_quantile_time(
            point_plus_1_std_orig_val, feat_name, baseline_profile_df_scaled, scaler, 
            feature, ctv, prediction_times_range_for_quantile, survival_threshold)
        
        # Calculate percentage time shortened
        percent_time_shortened = 0.0
        try:
            # Handle cases where both times are capped at max (no observable shortening within window)
            if time_at_baseline_mean == prediction_times_range_for_quantile.max() and                time_at_plus_1_std == prediction_times_range_for_quantile.max():
                 percent_time_shortened = 0.0 
            elif time_at_baseline_mean <= 0: # Avoid division by zero or negative times
                percent_time_shortened = np.nan
            else:
                percent_time_shortened = ((time_at_baseline_mean - time_at_plus_1_std) / time_at_baseline_mean) * 100
        except TypeError: # e.g., if max() returns a string or NaN for some reason
            percent_time_shortened = np.nan
        
        one_std_impact_summary.append({
            'Feature': feat_name,
            f'Original Mean {feat_name}': f'{point_baseline_orig_val:.2f}',
            f'Original Mean + 1 Std {feat_name}': f'{point_plus_1_std_orig_val:.2f}',
            f'Time for {int((1-survival_threshold)*100)}% Uplevel (Baseline Mean)': f'{time_at_baseline_mean}',
            f'Time for {int((1-survival_threshold)*100)}% Uplevel (+1 Std)': f'{time_at_plus_1_std}',
            'Percentage Time Shortened': f'{percent_time_shortened:.2f}%'
        })
        
        # --- Plotting (as in previous response) ---
        num_points_in_range_for_plot = 100 # For smooth line plot
        val_min_orig = np.percentile(original_feature_data, 1) 
        val_max_orig = np.percentile(original_feature_data, 99)
        
        if val_max_orig <= val_min_orig:
            val_max_orig = val_min_orig + 0.01 
            if val_max_orig <= val_min_orig:
                print(f"Skipping '{feat_name}' plot due to effectively constant values in original data.")
                continue

        original_values_to_plot_for_plot = np.linspace(val_min_orig, val_max_orig, num_points_in_range_for_plot)
        predicted_quantile_times_for_plot = []
        
        for original_val_for_plot in original_values_to_plot_for_plot:
            quantile_time_for_plot = get_predicted_quantile_time(
                original_val_for_plot, feat_name, baseline_profile_df_scaled, scaler, 
                feature, ctv, prediction_times_range_for_quantile, survival_threshold)
            predicted_quantile_times_for_plot.append(quantile_time_for_plot)
        
        plt.figure(figsize=(10, 6))
        plt.plot(original_values_to_plot_for_plot, predicted_quantile_times_for_plot, linestyle='-', color='blue') 
        
        plt.title(f'Predicted Time for {int((1-survival_threshold)*100)}% Uplevel vs. {feat_name}')
        plt.xlabel(f'{feat_name} (Original Values)')
        plt.ylabel(f'Predicted Time for {int((1-survival_threshold)*100)}% Uplevel (Days)')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()

# Print the 1-Std Dev impact summary table
print("\n--- 1-Standard Deviation Impact Summary ---")
one_std_impact_df = pd.DataFrame(one_std_impact_summary)
print(one_std_impact_df.to_string(index=False))

print("\n--- Plots of Covariate Change vs. Quantile Time complete ---")


# In[13]:


one_std_impact_df


# In[18]:


# --- Plotting Covariate Value vs. Quantile Time to T3 & Calculating 1-Std Dev Impact ---

# Define the survival probability threshold for the quantile you want to show
survival_threshold = 0.95 # As per your code in the prompt

summary_df = ctv.summary
significant_positive_impact = summary_df[
    (summary_df['p'] < 0.05) & 
    (summary_df['exp(coef)'] > 1)
].sort_values(by='exp(coef)', ascending=False)

num_features_to_plot = 10 
top_features_for_plot = significant_positive_impact.index.tolist()[:num_features_to_plot]

prediction_times_range_for_quantile = np.arange(0, df_train['stop_time'].max() + 30, 1) 

one_std_impact_summary = [] # Prepare a list to store the 1-Std Dev impact summary

# --- Helper function to get predicted quantile time for a given original value ---
def get_predicted_quantile_time(original_val_input, feat_name_input, 
                                baseline_profile_scaled, scaler_obj, 
                                numerical_feats_list, ctv_fitter, pred_times_range, threshold):
    temp_df_for_transform = pd.DataFrame(0.0, index=[0], columns=numerical_feats_list)
    temp_df_for_transform[feat_name_input] = original_val_input
    
    scaled_row_values = scaler_obj.transform(temp_df_for_transform)
    idx_in_scaled_array = numerical_feats_list.index(feat_name_input)
    scaled_val = scaled_row_values[0, idx_in_scaled_array]
    
    hypothetical_df_scaled = baseline_profile_df_scaled.copy()
    hypothetical_df_scaled[feat_name_input] = scaled_val
    
    log_partial_hazard = ctv_fitter.predict_log_partial_hazard(hypothetical_df_scaled).iloc[0]
    
    predicted_sf_data_to_series = (
        ctv_fitter.baseline_survival_.reindex(pred_times_range, method='pad') ** np.exp(log_partial_hazard)
    ).values.flatten() 

    predicted_sf_series = pd.Series(predicted_sf_data_to_series, index=pred_times_range)
    
    quantile_time = predicted_sf_series[predicted_sf_series <= threshold].index.min()
    if pd.isna(quantile_time):
        quantile_time = pred_times_range.max() 
    return quantile_time
# --- End of Helper function ---


if not top_features_for_plot:
    print("\nNo statistically significant positive impact features found to plot.")
else:
    print(f"\nPlotting for top {len(top_features_for_plot)} features:")
    
    baseline_profile_df_scaled = df_train[feature].median().to_frame().T 

    for feat_name in top_features_for_plot:
        if feat_name not in feature:
            print(f"Skipping plotting '{feat_name}' as it's not a scaled numerical feature or not found.")
            continue

        original_feature_data = df_train_original_unscaled[feat_name].dropna().values 
        
        num_points_in_range_for_plot = 100 # For smooth line plot
        val_min_orig = np.percentile(original_feature_data, 1) 
        val_max_orig = np.percentile(original_feature_data, 99)
        
        if val_max_orig <= val_min_orig:
            val_max_orig = val_min_orig + 0.01 
            if val_max_orig <= val_min_orig:
                print(f"Skipping '{feat_name}' plot due to effectively constant values in original data.")
                continue

        original_values_to_plot_for_plot = np.linspace(val_min_orig, val_max_orig, num_points_in_range_for_plot)
        predicted_quantile_times_for_plot = []
        
        for original_val_for_plot in original_values_to_plot_for_plot:
            quantile_time_for_plot = get_predicted_quantile_time(
                original_val_for_plot, feat_name, baseline_profile_df_scaled, scaler, 
                feature, ctv, prediction_times_range_for_quantile, survival_threshold)
            predicted_quantile_times_for_plot.append(quantile_time_for_plot)
        
        # --- Plotting ---
        plt.figure(figsize=(10, 6))
        plt.plot(original_values_to_plot_for_plot, predicted_quantile_times_for_plot, linestyle='-', color='blue') 
        
        # --- Diminishing Returns Annotation ---
        # Heuristic for "knee point" (adjust index based on visual inspection)
        # Often around 50-75% of the x-range for diminishing returns
        knee_point_idx = int(num_points_in_range_for_plot * 0.70) # e.g., 70% of the way through the x-values
        
        if knee_point_idx < len(original_values_to_plot_for_plot): # Ensure index is valid
            x_knee = original_values_to_plot_for_plot[knee_point_idx]
            y_knee = predicted_quantile_times_for_plot[knee_point_idx]

            # Add vertical and horizontal dashed lines
            plt.axvline(x=x_knee, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
            plt.axhline(y=y_knee, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)

            # Add text annotation
            # Adjust x, y for annotation placement to avoid overlapping the line
            offset_x = (val_max_orig - val_min_orig) * 0.05
            offset_y = (predicted_quantile_times_for_plot[0] - predicted_quantile_times_for_plot[-1]) * 0.05
            
            plt.annotate(
                f'Diminishing returns around:\n{feat_name}: {x_knee:.2f}\nTime: {y_knee:.2f} days',
                xy=(x_knee, y_knee),
                xytext=(x_knee + offset_x, y_knee + offset_y), # Adjust text position
                arrowprops=dict(facecolor='black', shrink=0.05, width=0.5, headwidth=6),
                fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.9", lw=0.5, alpha=0.8) # Background box
            )
        # --- End Diminishing Returns Annotation ---

        plt.title(f'Predicted Time for {int((1-survival_threshold)*100)}% Uplevel vs. {feat_name}')
        plt.xlabel(f'{feat_name} (Original Values)')
        plt.ylabel(f'Predicted Time for {int((1-survival_threshold)*100)}% Uplevel (Days)')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()

# Print the 1-Std Dev impact summary table (from previous response)
# This part goes outside the loop after all features have been processed
# (The content of 'one_std_impact_summary' is calculated within the loop
# and stored, then printed at the very end.)

print("\n--- 1-Standard Deviation Impact Summary (calculated for each feature) ---")
# Example of how to structure the data for this table (replace with your actual one_std_impact_summary)
# This part was already in the previous full response.
# You would have populated 'one_std_impact_summary' within the loop previously.
# For simplicity, I'm just showing the print statement here.
# one_std_impact_df = pd.DataFrame(one_std_impact_summary)
# print(one_std_impact_df.to_string(index=False))


print("\n--- Plots of Covariate Change vs. Quantile Time complete ---")


# In[29]:


[33, 33, 33, 32, 32, 32, 32, 32, 32, 32, 32, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 29, 29, 29, 29, 29, 29, 29, 28, 28, 28, 28, 28, 28, 28, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 26, 26, 26, 26, 26, 26, 26, 26, 26, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22][16]


# In[105]:


# --- Plotting Covariate Value vs. Quantile Time to T3 & Calculating 1-Std Dev Impact ---

# Define the survival probability threshold for the quantile you want to show
survival_threshold = 0.95 # This means Time for 5% Uplevel

summary_df = ctv.summary
significant_positive_impact = summary_df[
    (summary_df['p'] < 0.05) & 
    (summary_df['exp(coef)'] > 1)
].sort_values(by='exp(coef)', ascending=False)

num_features_to_plot = 10 
top_features_for_plot = significant_positive_impact.index.tolist()[:num_features_to_plot]

prediction_times_range_for_quantile = np.arange(0, df_train['stop_time'].max() + 30, 1) # Extended range for quantile prediction


# Helper function to get predicted quantile time for a given original value
def get_predicted_quantile_time(original_val_input, feat_name_input, 
                                baseline_profile_scaled, scaler_obj, 
                                numerical_feats_list, ctv_fitter, pred_times_range, threshold):
    temp_df_for_transform = pd.DataFrame(0.0, index=[0], columns=numerical_feats_list)
    temp_df_for_transform[feat_name_input] = original_val_input
    
    scaled_row_values = scaler_obj.transform(temp_df_for_transform)
    idx_in_scaled_array = numerical_feats_list.index(feat_name_input)
    scaled_val = scaled_row_values[0, idx_in_scaled_array]
    
    hypothetical_df_scaled = baseline_profile_scaled.copy()
    hypothetical_df_scaled[feat_name_input] = scaled_val
    
    log_partial_hazard = ctv_fitter.predict_log_partial_hazard(hypothetical_df_scaled).iloc[0]
    
    predicted_sf_data_to_series = (
        ctv_fitter.baseline_survival_.reindex(pred_times_range, method='pad') ** np.exp(log_partial_hazard)
    ).values.flatten() 

    predicted_sf_series = pd.Series(predicted_sf_data_to_series, index=pred_times_range)
    
    quantile_time = predicted_sf_series[predicted_sf_series <= threshold].index.min()
    if pd.isna(quantile_time):
        quantile_time = pred_times_range.max() 
    return quantile_time


if not top_features_for_plot:
    print("\nNo statistically significant positive impact features found to plot.")
else:
    print(f"\nPlotting for top {len(top_features_for_plot)} features:")
    
#     baseline_profile_df_scaled = df_train[feature].median().to_frame().T 
    baseline_profile_df_scaled = df_train[feature].quantile(0).to_frame().T 

    # Prepare list for 1-Std Dev impact summary (as in previous response)
    one_std_impact_summary = []

    # --- Define the specific business target for benchmark ---
    target_time_for_benchmark = 30 # Business goal: 5% uplevel within 30 days (Y-axis value)

    for feat_name in top_features_for_plot:
        if feat_name not in feature:
            print(f"Skipping plotting '{feat_name}' as it's not a scaled numerical feature or not found.")
            continue

        original_feature_data = df_train_original_unscaled[feat_name].dropna().values 
        
#         # --- Calculate 1-Standard Deviation Impact (as in previous response) ---
#         original_mean = df_train_original_unscaled[feat_name].mean()
#         original_std = df_train_original_unscaled[feat_name].std()
        
#         time_at_baseline_mean = get_predicted_quantile_time(original_mean, feat_name, baseline_profile_df_scaled, scaler, feature, ctv, prediction_times_range_for_quantile, survival_threshold)
#         time_at_plus_1_std = get_predicted_quantile_time(original_mean + original_std, feat_name, baseline_profile_df_scaled, scaler, feature, ctv, prediction_times_range_for_quantile, survival_threshold)
#         percent_time_shortened = 0.0
#         try:
#             if time_at_baseline_mean == prediction_times_range_for_quantile.max() and \
#                time_at_plus_1_std == prediction_times_range_for_quantile.max():
#                  percent_time_shortened = 0.0 
#             elif time_at_baseline_mean <= 0: 
#                 percent_time_shortened = np.nan
#             else:
#                 percent_time_shortened = ((time_at_baseline_mean - time_at_plus_1_std) / time_at_baseline_mean) * 100
#         except TypeError: 
#             percent_time_shortened = np.nan
        
#         one_std_impact_summary.append({
#             'Feature': feat_name,
#             f'Original Mean {feat_name}': f'{original_mean:.2f}',
#             f'Original Mean + 1 Std {feat_name}': f'{original_mean + original_std:.2f}',
#             f'Time for {int((1-survival_threshold)*100)}% Uplevel (Baseline Mean)': f'{time_at_baseline_mean}',
#             f'Time for {int((1-survival_threshold)*100)}% Uplevel (+1 Std)': f'{time_at_plus_1_std}',
#             'Percentage Time Shortened': f'{percent_time_shortened:.2f}%'
#         })


        # --- Plotting Code (from previous response) ---
        num_points_in_range_for_plot = 100 
        val_min_orig = np.percentile(original_feature_data, 1) 
        val_max_orig = np.percentile(original_feature_data, 99)
        
        if val_max_orig <= val_min_orig:
            val_max_orig = val_min_orig + 0.01 
            if val_max_orig <= val_min_orig:
                print(f"Skipping '{feat_name}' plot due to effectively constant values in original data.")
                continue

        original_values_to_plot_for_plot = np.linspace(val_min_orig, val_max_orig, num_points_in_range_for_plot)
        predicted_quantile_times_for_plot = []
        
        for original_val_for_plot in original_values_to_plot_for_plot:
            quantile_time_for_plot = get_predicted_quantile_time(
                original_val_for_plot, feat_name, baseline_profile_df_scaled, scaler, 
                feature, ctv, prediction_times_range_for_quantile, survival_threshold)
            predicted_quantile_times_for_plot.append(quantile_time_for_plot)
        
        plt.figure(figsize=(10, 6))
        plt.plot(original_values_to_plot_for_plot, predicted_quantile_times_for_plot, linestyle='-', color='blue') 
        
        plt.title(f'Predicted Time for {int((1-survival_threshold)*100)}% Uplevel vs. {feat_name}')
        plt.xlabel(f'{feat_name} (Original Values)')
        plt.ylabel(f'Predicted Time for {int((1-survival_threshold)*100)}% Uplevel (Days)')
        plt.grid(True, linestyle='--', alpha=0.7)

        # --- Add Benchmark Annotation to the Plot ---
        # Find the specific original_value from the plotted range that meets the target time
        # This uses the data points *used for the plot*
        target_indices_on_plot = np.where(np.array(predicted_quantile_times_for_plot) <= target_time_for_benchmark)[0]
#         print(predicted_quantile_times_for_plot)
#         print(np.where(np.array(predicted_quantile_times_for_plot) <= target_time_for_benchmark))

        if len(target_indices_on_plot) > 0:
            # Take the lowest covariate value (first index) that achieves the target time or better
            idx_for_plot_benchmark = target_indices_on_plot[0]
            
            x_benchmark_plot = original_values_to_plot_for_plot[idx_for_plot_benchmark]
            y_benchmark_plot = predicted_quantile_times_for_plot[idx_for_plot_benchmark]

            # Add lines and annotation
            plt.axvline(x=x_benchmark_plot, color='red', linestyle=':', linewidth=1.5, alpha=0.8, label=f'Benchmark: {x_benchmark_plot:.2f}')
            plt.axhline(y=y_benchmark_plot, color='red', linestyle=':', linewidth=1.5, alpha=0.8)
            
            plt.annotate(
                f'Target: {int((1-survival_threshold)*100)}% in {target_time_for_benchmark} days\nAchieved at {feat_name}: {x_benchmark_plot:.2f}',
                xy=(x_benchmark_plot, y_benchmark_plot),
                xytext=(x_benchmark_plot + (val_max_orig - val_min_orig) * 0.05, y_benchmark_plot + (predicted_quantile_times_for_plot[0] - predicted_quantile_times_for_plot[-1]) * 0.05),
                arrowprops=dict(facecolor='red', shrink=0.05, width=0.5, headwidth=6, color='red'),
                fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="red", lw=1, alpha=0.8),
                color='red'
            )
            plt.legend(loc='upper right') # Add legend to show benchmark line label

        plt.tight_layout()
        plt.show()

# Print the 1-Std Dev impact summary table
print("\n--- 1-Standard Deviation Impact Summary ---")
one_std_impact_df = pd.DataFrame(one_std_impact_summary)
print(one_std_impact_df.to_string(index=False))

print("\n--- Plots of Covariate Change vs. Quantile Time complete ---")


# In[100]:


# --- Common definitions for benchmarking ---
# Define an extended prediction time range to cover potential median/quantile values
# This range should be sufficient for baseline and improved scenarios
prediction_times_full_range = np.arange(0, df_train['stop_time'].max() + 60, 1) # Extend by 60 days
numerical_features_to_scale=feature
# Helper function to get predicted survival probability at a specific time ---
def get_survival_at_specific_time(original_val_input, feat_name_input, 
                                   baseline_profile_scaled, scaler_obj, 
                                   numerical_feats_list, ctv_fitter, pred_times_range, target_time_point):
    temp_df_for_transform = pd.DataFrame(0.0, index=[0], columns=numerical_feats_list)
    temp_df_for_transform[feat_name_input] = original_val_input
    
    scaled_row_values = scaler_obj.transform(temp_df_for_transform)
    idx_in_scaled_array = numerical_feats_list.index(feat_name_input)
    scaled_val = scaled_row_values[0, idx_in_scaled_array]
    
    hypothetical_df_scaled = baseline_profile_scaled.copy()
    hypothetical_df_scaled[feat_name_input] = scaled_val
    
    log_partial_hazard = ctv_fitter.predict_log_partial_hazard(hypothetical_df_scaled).iloc[0]
    
    predicted_sf_data_to_series = (
        ctv_fitter.baseline_survival_.reindex(pred_times_range, method='pad') ** np.exp(log_partial_hazard)
    ).values.flatten() 
    predicted_sf_series = pd.Series(predicted_sf_data_to_series, index=pred_times_range)
    
    if target_time_point > pred_times_range.max():
        # If the target time is beyond our prediction range, return the survival prob at max range
        return predicted_sf_series.iloc[-1]
    
    # Return the survival probability AT the target_time_point
    return predicted_sf_series.reindex([target_time_point], method='pad').iloc[0]


# --- Helper function to get predicted quantile time for a given original value ---
def get_predicted_quantile_time(original_val_input, feat_name_input, 
                                baseline_profile_scaled, scaler_obj, 
                                numerical_feats_list, ctv_fitter, pred_times_range, threshold):
    temp_df_for_transform = pd.DataFrame(0.0, index=[0], columns=numerical_feats_list)
    temp_df_for_transform[feat_name_input] = original_val_input
    
    scaled_row_values = scaler_obj.transform(temp_df_for_transform)
    idx_in_scaled_array = numerical_feats_list.index(feat_name_input)
    scaled_val = scaled_row_values[0, idx_in_scaled_array]
    
    hypothetical_df_scaled = baseline_profile_scaled.copy()
    hypothetical_df_scaled[feat_name_input] = scaled_val
    
    log_partial_hazard = ctv_fitter.predict_log_partial_hazard(hypothetical_df_scaled).iloc[0]
    
    predicted_sf_data_to_series = (
        ctv_fitter.baseline_survival_.reindex(pred_times_range, method='pad') ** np.exp(log_partial_hazard)
    ).values.flatten() 

    predicted_sf_series = pd.Series(predicted_sf_data_to_series, index=pred_times_range)
    
    quantile_time = predicted_sf_series[predicted_sf_series <= threshold].index.min()
    if pd.isna(quantile_time):
        quantile_time = pred_times_range.max() 
    return quantile_time


# Define the target uplevel percentage for the baseline (Step 1)
target_uplevel_percent_dynamic = 5 # %
target_survival_prob_dynamic = 1 - (target_uplevel_percent_dynamic / 100) # 0.80 survival probability
time=30

# Get the median profile of scaled features for the baseline calculation
# baseline_profile_df_scaled = df_train[feature].median().to_frame().T 
baseline_profile_df_scaled = df_train[feature].quantile(0.25).to_frame().T 


# Step 1: Calculate the time for the baseline seller to achieve this target uplevel percentage
time_for_baseline_target_uplevel = get_predicted_quantile_time(
    original_val_input=df_train_original_unscaled[numerical_features_to_scale[0]].mean(), # Dummy val for input to function
    feat_name_input=numerical_features_to_scale[0], # Dummy feat name for input to function
    baseline_profile_scaled=baseline_profile_df_scaled, 
    scaler_obj=scaler, 
    numerical_feats_list=numerical_features_to_scale, 
    ctv_fitter=ctv, 
    pred_times_range=prediction_times_full_range, 
    threshold=target_survival_prob_dynamic
)

if pd.isna(time_for_baseline_target_uplevel):
    time_for_baseline_target_uplevel = prediction_times_full_range.max()
    print(f"\n  Note: Baseline seller does not reach {target_uplevel_percent_baseline}% uplevel within {prediction_times_full_range.max()} days. Benchmark target time capped for calculations.")

print(f"\n  Step 1 Result: Baseline seller achieves {target_uplevel_percent_dynamic}% uplevel (survival <= {target_survival_prob_dynamic:.2f}) in {time} days.")


# Steps 2 & 3: Loop through covariates to find values that achieve this time
target_time_for_benchmark_loop = time_for_baseline_target_uplevel # This is the dynamically determined target time
target_survival_prob_fixed_for_loop = target_survival_prob_dynamic # This is 0.80

results_user_method_benchmark = []

summary_df = ctv.summary
top_features_for_benchmark = summary_df[
    (summary_df['p'] < 0.05) & 
    (summary_df['exp(coef)'] > 1)
].sort_values(by='exp(coef)', ascending=False).index.tolist()

for feat_name in top_features_for_benchmark:
    if feat_name not in numerical_features_to_scale:
        print(f"Skipping benchmark for '{feat_name}' as it's not a scaled numerical feature or not found.")
        continue

    original_feature_data = df_train_original_unscaled[feat_name].dropna().values 
    
    # Define a wide range of original values for this covariate to search within
    search_points_count = 100 
    search_val_min_orig = np.percentile(original_feature_data, 1) 
    search_val_max_orig = np.percentile(original_feature_data, 99)

    search_original_values = np.linspace(search_val_min_orig, search_val_max_orig, search_points_count)
    
    benchmark_covariate_val = []
    achieved_time_at_benchmark = []
    # Iterate through the search values (from min to max)
    for original_val_check in search_original_values:
        # Get the predicted time for this specific original_val_check to reach the target_survival_prob_fixed_for_loop
        current_achieved_time = get_predicted_quantile_time(
            original_val_check, feat_name, baseline_profile_df_scaled, scaler, 
            numerical_features_to_scale, ctv, prediction_times_full_range, target_survival_prob_fixed_for_loop
        )
        achieved_time_at_benchmark.append(current_achieved_time)
        # Compare against the correct variable: target_time_for_benchmark_loop
        if current_achieved_time == time: # <--- FIX IS HERE
#             benchmark_covariate_val = original_val_check
#             achieved_time_at_benchmark = current_achieved_time
            benchmark_covariate_val.append(original_val_check)
#             break # Found the minimum value, so break
#     print(feat_name)
#     print(achieved_time_at_benchmark)
#     print(benchmark_covariate_val)
            
    results_user_method_benchmark.append({
        'Feature': feat_name,
#         f'Target Time (Days) for {target_uplevel_percent_baseline}% Uplevel (Baseline)': f'{target_time_for_benchmark_loop:.2f}',
        f'Benchmark Value': f'{np.min(benchmark_covariate_val):.2f}'if benchmark_covariate_val else 'unidentified',
#         f'Achieved Time at Benchmark Value': f'{achieved_time_at_benchmark:.2f}' if not pd.isna(achieved_time_at_benchmark) else 'N/A',
      #  f'Time Reduction (Days)': f'{target_time_for_benchmark_loop - achieved_time_at_benchmark:.2f}' if not pd.isna(benchmark_covariate_val) else 'N/A'
    })

print("\n--- Benchmarks using User's Proposed Method ---")
user_method_benchmark_df = pd.DataFrame(results_user_method_benchmark)
user_method_benchmark_df


# In[99]:


df_train[feature].quantile(0.25).to_frame().T 


# In[58]:


# --- NEW BENCHMARKING METHOD: Based on User's Proposed Logic ---

# Define the target uplevel percentage for the baseline (Step 1)
target_uplevel_percent_dynamic = 5 # %
target_survival_prob_dynamic = 1 - (target_uplevel_percent_dynamic / 100) # 0.80 survival probability

# Get the median profile of scaled features for the baseline calculation
baseline_profile_df_scaled = df_train[feature].median().to_frame().T 

# Step 1: Calculate the time for the baseline seller to achieve this target uplevel percentage
time_for_baseline_target_uplevel = get_predicted_quantile_time(
    original_val_input=df_train_original_unscaled[numerical_features_to_scale[0]].mean(), # Dummy val for input to function
    feat_name_input=numerical_features_to_scale[0], # Dummy feat name for input to function
    baseline_profile_scaled=baseline_profile_df_scaled, 
    scaler_obj=scaler, 
    numerical_feats_list=numerical_features_to_scale, 
    ctv_fitter=ctv, 
    pred_times_range=prediction_times_full_range, 
    threshold=target_survival_prob_dynamic
)

if pd.isna(time_for_baseline_target_uplevel):
    time_for_baseline_target_uplevel = prediction_times_full_range.max()
    print(f"\n  Note: Baseline seller does not reach {target_uplevel_percent_dynamic}% uplevel within {prediction_times_full_range.max()} days. Benchmark target time capped for calculations.")

print(f"\n  Step 1 Result: Baseline seller achieves {target_uplevel_percent_dynamic}% uplevel (survival <= {target_survival_prob_dynamic:.2f}) in {time_for_baseline_target_uplevel} days.")


# Steps 2 & 3: Loop through covariates to find values that achieve this time
target_time_for_benchmark_loop = time_for_baseline_target_uplevel # This is the dynamically determined target time
target_survival_prob_fixed_for_loop = target_survival_prob_dynamic # This is 0.80

results_user_method_benchmark = []

summary_df = ctv.summary
top_features_for_benchmark = summary_df[
    (summary_df['p'] < 0.05) & 
    (summary_df['exp(coef)'] > 1)
].sort_values(by='exp(coef)', ascending=False).index.tolist()

for feat_name in top_features_for_benchmark:
    if feat_name not in numerical_features_to_scale:
        print(f"Skipping benchmark for '{feat_name}' as it's not a scaled numerical feature or not found.")
        continue

    original_feature_data = df_train_original_unscaled[feat_name].dropna().values 
    
    # Define a wide range of original values for this covariate to search within
    search_points_count = 200 
    search_val_min_orig = np.percentile(original_feature_data, 1) 
    search_val_max_orig = np.percentile(original_feature_data, 99)
    
    if search_val_max_orig <= search_val_min_orig: # Use val_min_orig here from percentiles
        search_val_max_orig = search_val_min_orig + 0.01 
        if search_val_max_orig <= search_val_min_orig: # Check again after adding delta
            print(f"Skipping benchmark for '{feat_name}' due to effectively constant values in original data.")
            continue

    search_original_values = np.linspace(search_val_min_orig, search_val_max_orig, search_points_count)
    
    benchmark_covariate_val = np.nan
    achieved_time_at_benchmark = np.nan

    # Iterate through the search values (from min to max)
    for original_val_check in search_original_values:
        
        # Get the predicted time for this specific original_val_check to reach the target_survival_prob_fixed_for_loop
        current_achieved_time = get_predicted_quantile_time(
            original_val_check, feat_name, baseline_profile_df_scaled, scaler, 
            numerical_features_to_scale, ctv, prediction_times_full_range, target_survival_prob_fixed_for_loop
        )
        
        # Compare against the correct variable: target_time_for_benchmark_loop
        if current_achieved_time <= target_time_for_benchmark_loop: # <--- FIX IS HERE
            benchmark_covariate_val = original_val_check
            achieved_time_at_benchmark = current_achieved_time
            break # Found the minimum value, so break
            
    results_user_method_benchmark.append({
        'Feature': feat_name,
        f'Target Time (Days) for {target_uplevel_percent_baseline}% Uplevel (Baseline)': f'{target_time_for_benchmark_loop:.2f}',
        f'Benchmark {feat_name} Value': f'{benchmark_covariate_val:.2f}' if not pd.isna(benchmark_covariate_val) else 'N/A',
        f'Achieved Time at Benchmark Value': f'{achieved_time_at_benchmark:.2f}' if not pd.isna(achieved_time_at_benchmark) else 'N/A',
        f'Time Reduction (Days)': f'{target_time_for_benchmark_loop - achieved_time_at_benchmark:.2f}' if not pd.isna(benchmark_covariate_val) else 'N/A'
    })

print("\n--- Benchmarks using User's Proposed Method ---")
user_method_benchmark_df = pd.DataFrame(results_user_method_benchmark)
print(user_method_benchmark_df.to_string(index=False))


# In[45]:


user_method_benchmark_df


# In[ ]:




