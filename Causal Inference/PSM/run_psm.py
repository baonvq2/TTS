import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import time

class PropensityScoreMatching:
    def __init__(self, df, treatment_features, confounders, targets, 
                 clip=0.01, max_distance=0.1, random_state=42,
                 n_estimators=100, max_depth=None, n_controls=1,
                 downsample_ratio=1.0, noise_level=1e-4):
        self.df = df.reset_index(drop=True)
        self.treatment_features = treatment_features
        self.original_confounders = confounders
        self.targets = targets
        self.clip = clip
        self.max_distance = max_distance
        self.random_state = random_state
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.n_controls = n_controls
        self.downsample_ratio = downsample_ratio
        self.noise_level = noise_level
        self.propensity_scores = None
        self.matched_indices = None
        self.encoded_confounders = None

    def preprocess_data(self):
        print("Preprocessing data...")
        start_time = time.time()
        
        # Downsample control group
        y = self.df[self.treatment_features[0]]  # Assuming binary treatment
        control = self.df[y == 0]
        n_control = len(control)
        n_control_sample = int(n_control * self.downsample_ratio)
        
        if n_control_sample < n_control:
            print(f"Downsampling control group from {len(control)} to {n_control_sample}")
            control_sample = control.sample(n=n_control_sample, random_state=self.random_state)
            self.df = pd.concat([self.df[y == 1], control_sample])
            self.df.reset_index(inplace=True)
        else:
            print("No downsampling needed")
            # X_balanced = X
            # y_balanced = y

            
        for feature in tqdm(self.original_confounders + self.targets, desc="Processing features"):
            if pd.api.types.is_categorical_dtype(self.df[feature]):
                # Handle categorical features
                missing_mask = self.df[feature].isna()
                self.df[feature] = self.df[feature].cat.add_categories('Missing')
                self.df.loc[missing_mask, feature] = 'Missing'
                
                value_counts = self.df[feature].value_counts(normalize=True)
                small_categories = value_counts[value_counts < 0.1].index
                self.df[feature] = self.df[feature].cat.add_categories('Other')
                self.df[feature] = self.df[feature].replace(small_categories, 'Other')
                
                # Remove unused categories
                self.df[feature] = self.df[feature].cat.remove_unused_categories()
            
            elif self.df[feature].dtype == 'object':
                # Handle non-categorical object columns
                self.df[feature] = self.df[feature].fillna('Missing')
                value_counts = self.df[feature].value_counts(normalize=True)
                small_categories = value_counts[value_counts < 0.1].index
                self.df[feature] = self.df[feature].replace(small_categories, 'Other')
            else:
                # Handle continuous features
                self.df[feature] = self.df[feature].fillna(0)

        print("Performing one-hot encoding...")
        # Perform one-hot encoding
        categorical_features = self.df[self.original_confounders].select_dtypes(include=['object', 'category']).columns
        encoded_df = pd.get_dummies(self.df[self.original_confounders], columns=categorical_features, drop_first=True)
        
        # Update the list of confounders after encoding
        self.encoded_confounders = encoded_df.columns.tolist()

        # Combine encoded features with non-categorical features
        self.df = pd.concat([self.df.drop(columns=self.original_confounders), encoded_df], axis=1)
        
        end_time = time.time()
        print(f"Preprocessing completed in {end_time - start_time:.2f} seconds")

    def calculate_propensity_scores(self):
        print("Calculating propensity scores...")
        start_time = time.time()
        
        X = self.df[self.encoded_confounders]
        y = self.df[self.treatment_features[0]]
        
        print("Training Random Forest model...")
        rf_model = RandomForestClassifier(n_estimators=self.n_estimators, 
                                          max_depth=self.max_depth, 
                                          random_state=self.random_state)
        rf_model.fit(X, y)
        
        print("Calculating feature importances...")
        importances = rf_model.feature_importances_
        forest_importances = pd.Series(importances, index=X.columns).to_frame()
        forest_importances.columns = ['importance']
        print("Top 10 most important features:")
        print(forest_importances.sort_values(by='importance', ascending=False).head(10))

        print("Predicting propensity scores...")
        self.propensity_scores = rf_model.predict_proba(X)[:, 1]
        self.propensity_scores = np.clip(self.propensity_scores, self.clip, 1 - self.clip)
        
        print("Adding noise to propensity scores...")
        # Add noise to propensity scores
        noise = np.random.uniform(-self.noise_level, self.noise_level, size=len(self.propensity_scores))
        self.propensity_scores += noise
        self.propensity_scores = np.clip(self.propensity_scores, self.clip, 1 - self.clip)
        
        end_time = time.time()
        print(f"Propensity score calculation completed in {end_time - start_time:.2f} seconds")

    def match_samples(self):
        print("Matching samples...")
        start_time = time.time()
        
        treated_indices = self.df[self.df[self.treatment_features[0]] == 1].index
        control_indices = self.df[self.df[self.treatment_features[0]] == 0].index

        treated_scores = self.propensity_scores[treated_indices]
        control_scores = self.propensity_scores[control_indices]

        matches = []

        # Create progress bar
        pbar = tqdm(total=len(treated_scores), desc="Matching samples", unit="sample")

        for i, treated_score in enumerate(treated_scores):
            distances = np.abs(control_scores + np.random.rand(len(control_scores))/100000 - treated_score)
            sorted_indices = np.argsort(distances)
            
            matched_controls = []
            for idx in sorted_indices:
                if distances[idx] <= self.max_distance:
                    matched_controls.append(control_indices[idx])
                    if len(matched_controls) == self.n_controls:
                        break
            
            if matched_controls:
                for control_idx in matched_controls:
                    matches.append((treated_indices[i], control_idx))
            
            # Update progress bar
            pbar.update(1)
            pbar.set_postfix({"Matched": len(matches)}, refresh=True)

        pbar.close()

        self.matched_indices = matches
        
        end_time = time.time()
        print(f"Matching completed in {end_time - start_time:.2f} seconds")
        print(f"Total matches: {len(matches)}")
        print(f"Unique treated samples matched: {len(set(m[0] for m in matches))}")
        print(f"Unique control samples used: {len(set(m[1] for m in matches))}")

    def perform_t_test(self):
        print("Performing t-tests...")
        start_time = time.time()
        
        results = {}
        for target in tqdm(self.targets, desc="T-tests"):
            self.df['matched_samples'] = 0
            self.df.loc[[m[0] for m in self.matched_indices], 'matched_samples'] = 1
            self.df.loc[[m[1] for m in self.matched_indices], 'matched_samples'] = 1
            treated_values = self.df.loc[[m[0] for m in self.matched_indices], target]
            control_values = self.df.loc[[m[1] for m in self.matched_indices], target]
            
            t_stat, p_value = stats.ttest_ind(treated_values, control_values)
            
            treated_before = self.df[self.df[self.treatment_features[0]] == 1][target]
            control_before = self.df[self.df[self.treatment_features[0]] == 0][target]
            
            results[target] = {
                't_statistic': t_stat, 
                'p_value': p_value,
                'n_treated': len(treated_values),
                'n_control': len(control_values),
                'avg_treated_after': treated_values.mean(),
                'avg_control_after': control_values.mean(),
                #'avg_treated_before': treated_before.mean(),
                #'avg_control_before': control_before.mean(),
                'uplift_after': treated_values.mean() - control_values.mean(),
                'uplift_before': treated_before.mean() - control_before.mean()
            }
        
        end_time = time.time()
        print(f"T-tests completed in {end_time - start_time:.2f} seconds")
        return results

    def run_analysis(self):
        print("Starting analysis...")
        overall_start_time = time.time()
        
        self.preprocess_data()
        self.calculate_propensity_scores()
        self.match_samples()
        results = self.perform_t_test()
        
        overall_end_time = time.time()
        print(f"Total analysis time: {overall_end_time - overall_start_time:.2f} seconds")
        
        return results, self.df

def aggregate_psm_results(results_list):
    """
    Aggregate results from multiple runs of Propensity Score Matching.
    
    Parameters:
    results_list (list): A list of result dictionaries, each from a single run of PSM.
    
    Returns:
    dict: A dictionary containing the mean values for each metric across all runs.
    """
    # Initialize a dictionary to store all values for each metric
    aggregated_results = {}
    
    # Assume all dictionaries in the list have the same structure
    for target in results_list[0].keys():
        aggregated_results[target] = {}
        for metric in results_list[0][target].keys():
            aggregated_results[target][metric] = []
    
    # Collect all values for each metric
    for result in results_list:
        for target, metrics in result.items():
            for metric, value in metrics.items():
                aggregated_results[target][metric].append(value)
    
    # Calculate mean for each metric
    mean_results = {}
    for target, metrics in aggregated_results.items():
        mean_results[target] = {metric: np.mean(values) for metric, values in metrics.items()}
    
    return mean_results

# Example usage:
# Assuming you have a list of 50 result dictionaries named 'all_results'
# all_results = [result1, result2, ..., result50]
# mean_results = aggregate_psm_results(all_results)

# To print the aggregated results in a readable format:
def print_aggregated_results(mean_results):
    for target, metrics in mean_results.items():
        print(f"\nResults for target: {target}")
        for metric, value in metrics.items():
            print(f"  {metric}: {value:.4f}")

def calculate_bootstrap_p_values(results_list):
    """
    Calculate p-values from bootstrap results.
    
    Parameters:
    results_list (list): A list of result dictionaries, each from a single bootstrap run.
    
    Returns:
    dict: A dictionary containing the mean effect sizes and corresponding p-values for each target.
    """
    bootstrap_results = {}
    
    for target in results_list[0].keys():
        # Extract uplift_after values for this target from all bootstrap samples
        uplifts = [result[target]['uplift_after'] for result in results_list]
        benchmark = [result[target]['avg_control_after'] for result in results_list]
        # Calculate mean effect size
        mean_effect = np.mean(uplifts)
        mean_benchmark = np.mean(benchmark)
        # Calculate standard error
        se = np.std([uplifts[ii]/mean_benchmark for ii in range(len(uplifts))], ddof=1)  # ddof=1 for sample standard deviation
        
        # Calculate z-score
        z_score = np.mean([uplifts[ii]/mean_benchmark for ii in range(len(uplifts))]) / se
        
        # Calculate two-tailed p-value
        p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
        
        bootstrap_results[target] = {
            'mean_effect': np.mean([uplifts[ii]/mean_benchmark for ii in range(len(uplifts))]),
            'val_control': np.mean([result[target]['avg_control_after'] for result in results_list]),
            'val_treatment': np.mean([result[target]['avg_treated_after'] for result in results_list]),
            'ss': np.mean([result[target]['n_treated'] for result in results_list]),
            'p_value': np.mean(p_value),
            'standard_error': se,
            'z_score': z_score,
        }
    
    return bootstrap_results

# Example usage:
# Assuming you have a list of result dictionaries named 'all_results'
# bootstrap_results = calculate_bootstrap_p_values(all_results)

def print_bootstrap_results(bootstrap_results):
    for target, metrics in bootstrap_results.items():
        print(f"\nResults for target: {target}")
        print(f"  Mean Effect: {metrics['mean_effect']:.4f}")
        print(f"  Standard Error: {metrics['standard_error']:.4f}")
        print(f"  Z-score: {metrics['z_score']:.4f}")
        print(f"  P-value: {metrics['p_value']:.4f}")

def plot_metric_distributions(dataframe_list, output_folder='./plots'):
    """
    Create distribution plots for each metric:
    - Plot the entire distribution in grey
    - Highlight the area between 2.5th and 97.5th percentiles in orange
    - Add a vertical line for the treated value
    - Position the title higher on the plot
    
    Args:
    dataframe_list (list): List of pandas DataFrames
    output_folder (str): Folder to save the plots (default: './plots')
    
    Returns:
    None
    """
    import os
    os.makedirs(output_folder, exist_ok=True)
    
    # Combine all dataframes
    combined_df = pd.concat(dataframe_list, ignore_index=False)
    
    # Get list of unique metrics
    metrics = combined_df.index.unique()

    #
    error_metrics = []
    
    for metric in metrics:
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Filter data for the current metric
            metric_data = combined_df.loc[metric]
            
            # Get the single value for avg_treated_after
            treated_value = np.mean(metric_data['avg_treated_after'])
            
            # Calculate the lower and upper bounds (2.5% and 97.5% percentiles)
            lower_bound, upper_bound = np.percentile(metric_data['avg_control_after'], [2.5, 97.5])
            
            # Calculate KDE manually
            kde = stats.gaussian_kde(metric_data['avg_control_after'])
            x_range = np.linspace(metric_data['avg_control_after'].min(), metric_data['avg_control_after'].max(), 1000)
            y_range = kde(x_range)
            
            # Plot the entire distribution in grey
            ax.fill_between(x_range, y_range, color='grey', alpha=0.5, label='Control')
            
            # Fill the area between 2.5th and 97.5th percentiles with orange
            ax.fill_between(x_range, y_range, where=(x_range >= lower_bound) & (x_range <= upper_bound), 
                            color='orange', alpha=0.6)
            
            # Add vertical line for treated
            ax.axvline(treated_value, color='blue', linestyle='--', label='Treated')
            
            # Set title with adjusted position
            ax.set_title(f'Distribution of {metric}', pad=20)  # Increase pad to move title higher
            
            ax.set_xlabel('Value')
            ax.set_ylabel('Density')
            ax.legend()
            
            # Add text annotations for percentiles
            ax.text(lower_bound, ax.get_ylim()[1], '2.5%', ha='center', va='bottom')
            ax.text(upper_bound, ax.get_ylim()[1], '97.5%', ha='center', va='bottom')
            
            # Adjust layout to prevent cutoff
            plt.tight_layout()
            
            # Save the plot
    #         plt.savefig(os.path.join(output_folder, f'{metric}_distribution.png'))
            plt.show()
        except np.linalg.LinAlgError as err:
            print(err)
            error_metrics.append(metric)
            continue
    print("  \n  \n  \n  \n  \nUnable to plot distribution graphs due to no difference:") 
    print(error_metrics)
