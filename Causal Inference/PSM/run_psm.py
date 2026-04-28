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
