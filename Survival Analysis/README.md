**Objective**

To understand which and how seller's actions could impact/speed up such progression time (reaching T3 level or above)

**Methodology**

  1. Objective: (1) to identify the key seller actions that significantly accelerate sellers' T3 upleveling time (from T2 to T3); (2) and to quantify the impact of these covariates
  2. Selected Model: Survival Analysis - CoxTimeVaryingFitter (Cox)
  3. Why choose Cox (Survival analysis)?
  At its core, the request revolves around how long it takes for sellers to achieve the T3 milestone and what factors influence this duration
    - Focus on Time-to-Event: Unlike traditional regression which might predict whether an event happens (classification) or a continuous value (regression). Cox models the duration of time until an event occurs, which is time spent in T2 until uplevelling occurs in this case
    - Handle time censoring: many sellers in TTS may not have reached T3 by the end of the observation (which is determined 60D herein). The survival analysis will treat these cases as the uplevelling event has not yet happened within the observed window, but it may happen later, which allows using all available data without bias introduced
  4. Why don't we use regular regression?
    Neither traditional regression method can accurately account for the dynamic risk of upleveling changing over time or based on a seller's evolving behavior
    - Linear Regression: attempt to predict continuous variable 'Time to T3', but cannot handle censored data (impossible to present 'never reached T3' numberically without bias). Also, distribution wise, linear regression makes assumptions that error distribution follows normality while time-to-event data is often skewed
    - Logistic Regression: predict proba of reaching T3 within 60D (binary outcome), but could not tell when sellers are likely to progress or how quickly sellers respond to any interventions
  5. Advantages and Optimzation function
    1. Advantages
      - No assumption of distribution for baseline upleveling for a 'standard' seller >> will be canceled out later (explained in 'model estimation')
      - Interpretable coefficients from the model (model output = exp(coef))
        - >1: an increase in a feature' value increases the likelihood of T3 upleveling, thus shortening the progression time (positive impact) >> we will focus on positive impact in this analysis
        - <1: an increase in a feature' value decreases the likelihood of T3 upleveling, thus lengthening the progression time (negative impact)
      - Handle Time-Varying issue: (1) sellers are not static, meaning their actions change over time while they gradually progress to T3; (2) Cox allows covariates to change values over different time intervals
    2. Model Estimation
    Cox models the hazard rate of a seller $$i$$at time given $$t$$, given their covariates $$X_i$$
      $$h(t|X_i) = h_0(t) exp(\beta_1 X_{i1} + \beta_2 X_{i2} + ... + \beta_p X_{ip})$$
- $$h(t|X_i)$$ : likelihood of T3 uplevelling for seller $$i$$at time $$t$$(aka hazard ratio)
- $$h_0(t)$$: baseline hazard, likelihood when all covariates = 0
- $$exp(\beta_1 X_{i1} + \beta_2 X_{i2} + ... + \beta_p X_{ip})$$ : hazard ratio multiplier, quantifying how sellers' action $$X$$proportionally impacts their uplevelling likelihood
    The model will aim to estimate the 'best' coefficient ($$\beta$$) to maximize the uplevelling likelihood. Cox will focus on the relative order of events, instead of trying to find $$h_0(t)$$: when an uplevelling event occurs at time $$t$$, the model will consider all sellers who are still in T2 at time (aka sellers at risk $$R(t_k)$$), to see how many sellers $$j$$ have upleveled, described as the below expression ratio
    $$L_k(\beta)=\frac{\text{upleveled sellers}}{\text{sellers at risk}} = \frac{h(t_k|X_j)}{\sum_{i{\epsilon}R(t_k)}h(t_k|X_i)}=\frac{h_0(t_k)exp(\sum_{m=1}^{p}\beta_mX_{jm})}{h_0(t_k)\sum_{i{\epsilon}R(t_k)}exp(\sum_{m=1}^{p}\beta_mX_{im})}\\=\frac{exp(\sum_{m=1}^{p}\beta_mX_{jm})}{\sum_{i{\epsilon}R(t_k)}exp(\sum_{m=1}^{p}\beta_mX_{im})}$$
Remark: $$h_0(t)$$is cancelled out in this expression
    Cox will seek to maximize the product of all $$L_k(\beta)$$at all observed events (the optimization similarly follows MLE for training)
$$L(\beta) = \prod_{k=1}^{\text{number of events}}L_k(\beta)$$

**Data Collection**
- Time censored: 60D at max
- Interval: 7-day interval >> used for data collection window
- Include both 'successfully achieved T3 sellers' and 'not yet achieved T3 sellers'
- Cohort: All GB sellers with first_t2_tier_date between 'yyyyMMdd' to 'yyyyMMdd'

<img width="643" height="241" alt="image" src="https://github.com/user-attachments/assets/cb0fae0b-4e86-4841-bbad-3922c6172f28" />


