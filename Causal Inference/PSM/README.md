**Context**
Instead of A/B Experiment, PSM is one of the approaches that are usually implemented to measure the causality of some features/initiatives in TTS platform

_PSM = propensity matching score

TTS = TikTokShop_

For example, in TTS, sellers aim to grow their GMV in the platform to reach to higher tiers. To achieve this, efforts from both sellers and platforms are required. One of the main initiatives for seller growth is formed as Missions (missions tell sellers to do XX contents per week/month, to upload more product, to connect with A/B/C content creators, etc.); on completion, sellers could receive the platform rewards and benefits from their actions to accelerate sellers’ growth
Methodology
 
 <img width="975" height="312" alt="image" src="https://github.com/user-attachments/assets/e62efec7-2903-4ba7-8505-3fc74c72ddaa" />



•	Treatment group: sellers who accept at least one mission in the program or in the mission group

•	Control group: simulated sellers via 100* Bootstrap + PSM based on confounding factors below


**Evaluation Methodology (Simulated A/B via Bootstrapping)**
After constructing treatment and control groups using Propensity Score Matching (PSM), we evaluate the causal impact by simulating an A/B testing framework through bootstrapping.

Key Idea

Since we do not have a true randomized experiment, we:

Re-sample the matched dataset multiple times
Compute the treatment effect repeatedly
Build an empirical distribution of the effect size
