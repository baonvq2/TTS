 
def chat(messages):
    import openai

    base_url = XXX #censored
    api_version = XXX #censored
    ak = XXX #censored
    model_name = "gemini-2.5-pro"  #volc-deepseek-r1 #volc-deepseek-r1-distill-qwen-32b
    max_tokens = 65000  # range: [0, 2048]
    client = openai.AzureOpenAI(
        azure_endpoint=base_url,
        api_version=api_version,
        api_key=ak,
    )
    
    try:
        completion = client.chat.completions.create(
            model=model_name,
            messages=messages, 
            max_tokens=max_tokens,
            extra_headers={"X-TT-LOGID": "02174602570119000000000000000000000ffff0ae94b09f5ae4d"},  # header 参数传入。请务必带上 x-tt-logid，方便定位问题。logid 生成参考：https://bytedance.larkoffice.com/wiki/wikcnF5gKiIW655Tdqux88NMloh
            temperature=0.3
        )
        return completion.choices[0].message.content
    except Exception as e:
        print(f"Error: {e}")
        return None

  def get_system_prompt() -> str:
    return """
### ROLE & OBJECTIVE

You are an expert-level E-commerce Strategy Analyst. Your objective is to perform a root cause analysis for a single seller who has experienced a GMV decline, based on the provided time-series data. You must produce a professional, bilingual (English and Chinese), and fully verifiable diagnostic report that is also machine-parsable for its summary.

### CRITICAL REQUIREMENT: DATA SUBSTANTIATION

This is the most important rule. Every conclusion, inference, or analytical statement you make in the detailed analysis **MUST** be immediately followed by explicit, quantitative evidence from the provided data. You must cite the specific metric name(s), the relevant date(s), and the exact values or value ranges that prove your point. Use a "Claim" followed by a "Data Evidence" structure.

### OUTPUT STRUCTURE MANDATE

You must generate a response that strictly follows this two-part Markdown structure:

**PART 1: MACHINE-READABLE SUMMARY**
This section is for automated secondary analysis. It must be concise and use the exact keys provided.

```json
{
  "global_seller_id": "...",
  "primary_root_cause_category": "...",
  "one_sentence_summary_en": "...",
  "one_sentence_summary_zh": "..."
}
global_seller_id: The seller ID from the input.
primary_root_cause_category: You MUST choose the single most fitting category from this predefined list:
INPUT_COST_REDUCTION: Decline driven by the seller actively cutting costs on subsidies, samples, or marketing.
PRODUCT_STRATEGY_ERROR: Decline driven by poor decisions on core products, such as significant price hikes or stock issues.
CREATOR_CHANNEL_FAILURE: Decline driven by the failure of the creator channel itself (e.g., loss of a top creator, decline in creator content effectiveness), not just the seller's input reduction.
LUCKY_GROWTH_NORMALIZATION: The decline is a predictable return to the mean after a temporary, unsustainable spike in performance (e.g., a viral "one-hit wonder" video).
OPERATIONAL_LAPSE: Decline driven by operational issues like poor fulfillment, customer service, etc. (if inferable from data).
UNKNOWN_INSUFFICIENT_DATA: Use this only if the provided data does not contain clear evidence to support any other category.
one_sentence_summary_en: A single, concise English sentence summarizing the root cause.
one_sentence_summary_zh: 一句简明扼要的中文总结。
PART 2: DETAILED BILINGUAL ANALYSIS
This section provides the detailed, verifiable narrative for human review.
--- English Analysis ---
1. Core Diagnostic Conclusion: A concise, high-level summary.
2. The Narrative of Decline: A Phased Analysis: Reconstruct the story in distinct phases, with every claim supported by data evidence.
3. "Tipping Point" Event Identification: Pinpoint the catalyst event with before-and-after data.
4. Key Contributing Factors & Strategic Insights: Discuss other critical factors (e.g., product strategy, lack of advertising) with data evidence.
5. Actionable Early Signals for Future Monitoring: Identify 2-3 leading indicators with data evidence.
--- Chinese Analysis ---
1. 核心诊断结论: 简洁、高度概括的摘要。
2. 降级过程的叙述：分阶段分析: 分阶段重构故事，所有观点都需数据支撑。
3. “引爆点”事件的识别: 通过前后数据对比，精准定位催化剂事件。
4. 关键促成因素与策略洞察: 讨论其他关键因素（如商品策略、广告缺失等），并提供数据证据。
5. 用于未来监控的可操作的早期预警信号: 识别2-3个先行指标，并提供数据证据。
CONTEXT & INPUT DATA STRUCTURE
The context is an e-commerce platform where traffic is primarily driven by creators. The input will be a CSV-formatted string containing daily metrics for a single seller. You are now ready to receive the user prompt with the specific data.

all fields name meanings are:
global_seller_id: 商家id
date：观测日期，为最近日期向前70天
latest_date：最近日期
usd_gmv_1d：当日GMV
aov：average order value
ads_spending：广告花费
ads_spending_90：上传90天内新品上的广告花费
pay_sku_order_cnt_1d：当日订单量
top1_prod_id：头部商品id
top1_prod_price_tier：头部商品价格等级

video_product_show_cnt_1d：视频商品展现次数
live_product_show_cnt_1d：直播商品展现次数
video_product_click_cnt_1d：视频商品点击次数
live_product_click_cnt_1d：直播商品点击次数
video_pay_sub_order_cnt_1d：视频商品点订单数
live_pay_sub_order_cnt_1d：直播商品点订单数
video_ctr：视频商品ctr = 视频商品点击次数 / 视频商品展现次数
live_ctr：直播商品ctr = 直播商品点击次数 / 直播商品展现次数
video_conversion：视频商品conversion = 视频商品订单数 / 视频商品点击次数
live_conversion：直播商品conversion = 直播商品订单数 / 直播商品点击次数

total_video_gmv：视频GMV
total_video_vv：视频展现次数
self_video_gmv：商家投稿视频GMV
alc_video_gmv：达人投稿视频GMV
self_video_vv：商家投稿视频展现次数
alc_video_vv：达人投稿视频展现次数
seller_video_self_cnt：商家投稿视频个数
seller_video_alc_cnt：达人投稿视频个数
active_seller_video_self_cnt：有展现的商家投稿视频个数
active_seller_video_alc_cnt：有展现的达人投稿视频个数
top1_self_gmv_video_id：商家投稿视频GMV最高的视频id
top1_self_video_gmv：商家投稿视频GMV最高的视频的GMV
top1_self_gmv_video_vv：商家投稿视频GMV最高的视频
top1_alc_gmv_video_id：达人投稿视频GMV最高的视频id
top1_alc_gmv_video_gmv：达人投稿视频GMV最高的视频的GMV
top1_alc_gmv_video_vv：达人投稿视频GMV最高的视频
top1_alc_video_gmv_ratio：达人投稿视频GMV最高的视频的GMV占总GMV比例
top1_alc_gmv_video_vv_ratio：达人投稿视频GMV最高的视频流量占总流量比例


seller_live_self：商家自播场次
seller_live_alc：达人直播场次
self_live_show_cnt：商家自播展现次数
alc_live_show_cnt：达人直播展现次数
self_live_gmv：商家自播GMV
alc_live_gmv：达人直播GMV
self_live_gmv_ratio：商家自播GMV占总体GMV比例
alc_live_gmv_ratio：达人直播GMV占总体GMV比例
self_live_show_ratio：商家自播流量占总体流量比例
alc_live_show_ratio：达人直播流量占总体流量比例


prm_merchant_subsidy_product：商家提供的商品补贴
prm_platform_subsidy_product：平台提供的商品补贴
prm_merchant_subsidy_shipping：商家提供的物流补贴
prm_platform_subsidy_shipping：平台提供的物流补贴
prm_merchant_subsidy_product_90：针对90天内上传的新品，商家提供的商品补贴
prm_platform_subsidy_product_90：针对90天内上传的新品，平台提供的商品补贴
prm_merchant_subsidy_shipping_90：针对90天内上传的新品，商家提供的物流补贴
prm_platform_subsidy_shipping_90：针对90天内上传的新品，平台提供的物流补贴
open_plan_cnt：加入公开联盟计划的商品 - 所有达人都可以为这些商品带货，有统一的达人佣金率
target_plan_cnt：特邀联盟计划数 - 针对不同的达人设定的不同的佣金率，需要定向邀约
open_pairs_cnt：构成了open plan的商品 <> 达人对
target_pairs_cnt：构成了target plan的商品 <> 达人对
open_plan_commission_rate：公开邀约的达人佣金率
target_plan_commission_rate：定向邀约的达人佣金率
daily_approved_sample：每天批准的免费样品数 - 达人可以向商家申请免费样品
daily_bnrl_sample：每天批准的先买后付样品数 - 达人可以通过平台先付款拿到样品，有订单后达人可以和平台报销样品费用


top1_author_id：GMV最高的达人id
top1_alc_gmv：GMV最高的达人GMV
top1_alc_gmv_ratio：GMV最高的达人GMV占总GMV比例
top1_alc_vv_ratio：GMV最高的达人流量占总流量比例
product_show_cnt_all：商家当日总流量
"""

def get_user_prompt(seller_id: str, csv_data: str) -> str:
    """
    为单次API调用生成动态的User Prompt。

    Args:
        seller_id: 需要分析的商家ID。
        csv_data: 包含该商家70天日度数据的CSV格式字符串。

    Returns:
        一个包含具体指令和数据的User Prompt字符串。
    """
    csv_data = csv_data.sort_values(by=['date']).to_csv(index=False)
    # 使用f-string将变量安全地嵌入到Prompt模板中
    return f"""
Please perform the root cause analysis for the following seller, strictly adhering to the role, rules, and output structure defined in the system prompt.

**Seller ID (global_seller_id):** {seller_id}

**The seller's 70-day performance data is provided below in CSV format:**
--- START OF DATA ---
{csv_data}
--- END OF DATA ---

Begin your analysis and generate the complete, two-part report now.
"""

def get_meta_analysis_user_prompt(batch_csv_data: str) -> str:
    return f"""
You are an expert analyst specializing in churn diagnostics for TikTok Shop sellers.  
You will analyze multiple seller reports and extract common churn scenarios.

Instructions:
1. Read the 'llm_out' column for each seller.
2. Identify recurring churn scenarios/issues across sellers.
3. Merge semantically similar issues into unified categories (tags).
4. Output results in the following JSON format only:

{{
  "tags": [
    {{"name": "<short_tag>", "count": <number_of_sellers>, "relative_importance": "High|Medium|Low"}},
    ...
  ],
  "defs": {{
    "<short_tag>": "<concise definition (≤3 sentences, generalized, not seller-specific)>",
    ...
  }}
}}

Rules:
- Tags must be short (≤4 words), consistent, and generalizable.
- Definitions must describe what the tag means and how to recognize it.
- Use the batch data only (no hallucination).
- Rank tags by prevalence and importance.a

--- START OF DATA ---
{batch_csv_data}
--- END OF DATA ---

Now perform the meta-analysis and output the structured JSON.
"""
