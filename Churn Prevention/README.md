**Context**
- Remaining in the T3+ tier after conversion seems to be a challenge

**Framework**

Initially, we explored using Traditional ML to generate common "voluntary reasons" that might prompt a seller to downgrade. However, this approach encountered several challenges, including the need for manual processes and potential data sparsity. These limitations led us to consider leveraging AI as an alternative solution.

<img width="652" height="372" alt="image" src="https://github.com/user-attachments/assets/fbc8af7c-aa1f-4be5-a805-924edaf798d4" />


<img width="1084" height="906" alt="image" src="https://github.com/user-attachments/assets/da6afdf3-a1d9-48d3-9e11-508f1a8e3b31" />
<img width="2481" height="964" alt="image" src="https://github.com/user-attachments/assets/d46f60b1-bb72-41dd-9d76-b4fc86051462" />

**Data fed to AI**

<img width="1072" height="633" alt="image" src="https://github.com/user-attachments/assets/48da2ca7-c7c8-42b8-b0f4-4d7c84df3757" />

**AI prompt design**

  - At high level:
    - Be specific about Objective: perform a root-cause diagnosis of GMV decline
    - Let the machine know your Role: Ecom Strategy Analyst
    - Construct a narrative of what happened and why, with clear phases and inflection points
    - Identify the primary cause of the decline and support it with data evidence
  - Key design:
    - Ground the model with a clear role: 'You are an expert-level E-commerce Strategy Analyst'
      - Decide model's tone and mindset
    - Set a precise objective: 'Your goal is to perform a root cause analysis of a GMV decline'
      - Be explicit about the business question you are trying to answer
    - Mandate evidence-based reasoning: 'Every conclusion MUST be followed by quantitative evidence from the data'
      - Force the model to tie every claim to actual metrics/evidence
    - Limit the scope: 'Choose ONE root cause from a predefined list of categories'
      - This avoids ambiguity and hallucination, also makes aggregation scalable across many sellers
      - Use 'UNKNOWN_INSUFFICIENT_DATA' if data is not sufficient to tell
    - Strict format: ensure consistency and scalability
