# ======================================================= 
# use data-pull.py and dsa_column_profile.csv to pull the data and inspect relevant elements
# =======================================================
import duckdb
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
from matplotlib.ticker import FuncFormatter

mpl.rcParams['font.family'] = 'Arial'
mpl.rcParams['font.size'] = 10
mpl.rcParams['axes.titlesize'] = 12
mpl.rcParams['axes.labelsize'] = 10

def millions(x, pos):
    return f'{x/1_000_000:.0f}mn'

con = duckdb.connect()

path = "data/tdb/aggregated-complete.parquet"

#inspect individual attribute to understand the different options outlined in the database; this will help run the search queries later

# for col in [
#     "source_type"
# ]:

#     print(f"\n===== {col} =====")

#     df = con.execute(f"""
#     SELECT
#         "{col}",
#         SUM(count) as total
#     FROM read_parquet('{path}')
#     GROUP BY "{col}"
#     ORDER BY total DESC
#     LIMIT 20
#     """).fetchdf()

#     print(df)


# =========================================================
# Settings
# =========================================================

#choosing the relevant platforms for the analysis

platform_filter_sql = """
CASE
    WHEN lower(platform_name) LIKE '%facebook%' THEN 'Facebook'
    WHEN lower(platform_name) LIKE '%linkedin%' THEN 'LinkedIn'
    WHEN lower(platform_name) LIKE '%youtube%' THEN 'YouTube'
    WHEN lower(platform_name) LIKE '%tiktok%' THEN 'TikTok'
    WHEN lower(platform_name) LIKE '%instagram%' THEN 'Instagram'
    WHEN lower(platform_name) LIKE '%snapchat%' THEN 'Snapchat'
    ELSE NULL
END
"""

qmul_blue = "#003865"

platform_colours = {
    "Facebook": "#003865",   # QMUL blue
    "LinkedIn": "#005EB8",   # brighter institutional blue
    "YouTube": "#A61C3C",    # muted crimson
    "TikTok": "#008B8B",     # teal
    "Instagram": "#7B2D26",  # muted burgundy
    "Snapchat": "#C69214",   # academic gold
}

monthly_active_users = {
    "Facebook": 259_000_000,
    "LinkedIn": 45_200_000,
    "YouTube": 416_600_000,
    "TikTok": 135_900_000,
    "Instagram": 259_000_000,
    "Snapchat": 102_000_000
}

# =========================================================
# 1. Total removal/disable decisions by selected platform
# =========================================================

removals_by_platform = con.execute(f"""
SELECT
    {platform_filter_sql} AS platform,
    SUM(count) AS moderation_actions
FROM read_parquet('{path}')
WHERE
    (
        DECISION_VISIBILITY_CONTENT_REMOVED = TRUE
        OR DECISION_VISIBILITY_CONTENT_DISABLED = TRUE
    )
    AND content_date >= DATE '2026-01-01'
    AND {platform_filter_sql} IS NOT NULL
GROUP BY platform
ORDER BY moderation_actions DESC
""").fetchdf()

removals_by_platform.to_csv(
    "output/moderation_actions_by_platform.csv",
    index=False
)

print(removals_by_platform)

removals_by_platform["actions_per_million_users"] = (
    removals_by_platform["moderation_actions"]
    / removals_by_platform["platform"].map(monthly_active_users)
) * 1_000_000

plt.figure(figsize=(5.8, 3.6))

plt.barh(
    removals_by_platform["platform"],
    removals_by_platform["actions_per_million_users"],
    color=[
        platform_colours.get(p, qmul_blue)
        for p in removals_by_platform["platform"]
    ]
)

plt.xlabel("Removal / disable decisions per 1 million monthly users")
plt.ylabel("Platform")
plt.title("Content Removal / Disable Decisions per 1 Million Monthly Users")
plt.gca().invert_yaxis()
plt.xscale("log")
plt.tight_layout()
plt.savefig("output/moderation_actions_by_platform.png", dpi=300)
plt.show()


# =========================================================
# 2. Time series by content date
# =========================================================

timechart = con.execute(f"""
SELECT
    DATE_TRUNC('month', application_date) AS application_month,
    {platform_filter_sql} AS platform,
    SUM(count) AS moderation_actions
FROM read_parquet('{path}')
WHERE
    (
        DECISION_VISIBILITY_CONTENT_REMOVED = TRUE
        OR DECISION_VISIBILITY_CONTENT_DISABLED = TRUE
    )
    AND application_date >= DATE '2024-01-01'
    AND application_date < DATE '2026-05-01'
    AND {platform_filter_sql} IS NOT NULL
GROUP BY application_month, platform
ORDER BY application_month, platform
""").fetchdf()

timechart.to_csv(
    "output/moderation_actions_timechart_application_date.csv",
    index=False
)

print(timechart)

timechart["actions_per_million_users"] = (
    timechart["moderation_actions"]
    / timechart["platform"].map(monthly_active_users)
) * 1_000_000

print(timechart[[
    "platform",
    "moderation_actions",
    "actions_per_million_users"
]].head())

timechart["content_month"] = pd.to_datetime(timechart["application_month"])

timechart_wide = timechart.pivot_table(
    index="application_month",
    columns="platform",
    values="actions_per_million_users",
    aggfunc="sum",
    fill_value=0
)

# Replace tiny values with NaN for cleaner log-scale plotting
#timechart_wide = timechart_wide.mask(timechart_wide < 0.1)

plt.figure(figsize=(5.8, 3.6))

for platform in [
    "Facebook",
    "LinkedIn",
    "YouTube",
    "TikTok",
    "Instagram",
    "Snapchat"
]:
    if platform in timechart_wide.columns:
        plt.plot(
            timechart_wide.index,
            timechart_wide[platform],
            marker="o",
            label=platform,
            color=platform_colours.get(platform, qmul_blue)
        )

plt.xlabel("Application month")
plt.ylabel("Removal / disable decisions per 1 million monthly users")
plt.title("Content Removal / Disable Decisions per 1 Million Monthly Users")
plt.legend()
plt.xticks(rotation=45)
plt.yscale("log")
plt.tight_layout()
plt.savefig("output/moderation_actions_timechart_application_date.png", dpi=300)
plt.show()

# =========================================================
# 3. Automated vs non-automated decisions
# =========================================================

automation_chart = con.execute(f"""
SELECT
    automated_decision,
    SUM(count) AS moderation_actions
FROM read_parquet('{path}')
WHERE
    application_date >= DATE '2026-01-01'
    AND application_date < DATE '2026-05-01'
    AND automated_decision IN (
        'AUTOMATED_DECISION_FULLY',
        'AUTOMATED_DECISION_PARTIALLY',
        'AUTOMATED_DECISION_NOT_AUTOMATED'
    )
GROUP BY automated_decision
ORDER BY moderation_actions DESC
""").fetchdf()

automation_chart.to_csv(
    "output/automation_decisions.csv",
    index=False
)

print(automation_chart)

automation_chart["automation_label"] = automation_chart["automated_decision"].replace({
    "AUTOMATED_DECISION_FULLY": "Fully automated",
    "AUTOMATED_DECISION_PARTIALLY": "Partially automated",
    "AUTOMATED_DECISION_NOT_AUTOMATED": "Not automated"
})

automation_colours = {
    "Fully automated": "#003865",
    "Partially automated": "#008B8B",
    "Not automated": "#C69214"
}

plt.figure(figsize=(5.8, 3.6))

plt.bar(
    automation_chart["automation_label"],
    automation_chart["moderation_actions"],
    color=[
        automation_colours.get(a, qmul_blue)
        for a in automation_chart["automation_label"]
    ]
)

plt.xlabel("Automation category")
plt.ylabel("Moderation decisions")
plt.title("Automated vs Non-Automated Moderation Decisions")
plt.gca().yaxis.set_major_formatter(FuncFormatter(millions))
plt.tight_layout()
plt.savefig("output/automation_decisions.png", dpi=300)
plt.show()

# =========================================================
# 4. Reason for content removal
# =========================================================

removal_reason_chart = con.execute(f"""
SELECT
    decision_ground,
    SUM(count) AS moderation_actions
FROM read_parquet('{path}')
WHERE
    application_date >= DATE '2026-01-01'
    AND application_date < DATE '2026-05-01'
    AND decision_ground IN (
        'DECISION_GROUND_INCOMPATIBLE_CONTENT',
        'DECISION_GROUND_ILLEGAL_CONTENT'
    )
GROUP BY decision_ground
ORDER BY moderation_actions DESC
""").fetchdf()

removal_reason_chart.to_csv(
    "output/removal_reason_decisions.csv",
    index=False
)

print(removal_reason_chart)

removal_reason_chart["reason_label"] = removal_reason_chart["decision_ground"].replace({
    "DECISION_GROUND_INCOMPATIBLE_CONTENT": "Incompatible content",
    "DECISION_GROUND_ILLEGAL_CONTENT": "Illegal content"
})

reason_colours = {
    "Incompatible content": "#003865",
    "Illegal content": "#A61C3C"
}

plt.figure(figsize=(5.8, 3.6))

plt.bar(
    removal_reason_chart["reason_label"],
    removal_reason_chart["moderation_actions"],
    color=[
        reason_colours.get(r, qmul_blue)
        for r in removal_reason_chart["reason_label"]
    ]
)

plt.xlabel("Decision ground")
plt.ylabel("Moderation decisions")
plt.title("Reason for Content Removal Decisions")

plt.yscale("log")

plt.tight_layout()

plt.savefig(
    "output/removal_reason_decisions.png",
    dpi=300
)

plt.show()

# =========================================================
# 5. Statement categories for removed / disabled content (each statement must provide one category, but can have multiple keywords; the categories are broader than the keywords, so this provides a higher-level overview of the types of content removed/disabled)
# =========================================================

statement_categories = {
    "STATEMENT_CATEGORY_ANIMAL_WELFARE": "Animal welfare",
    "STATEMENT_CATEGORY_DATA_PROTECTION_AND_PRIVACY_VIOLATIONS": "Data protection & privacy",
    "STATEMENT_CATEGORY_ILLEGAL_OR_HARMFUL_SPEECH": "Illegal or harmful speech",
    "STATEMENT_CATEGORY_INTELLECTUAL_PROPERTY_INFRINGEMENTS": "IP infringements",
    "STATEMENT_CATEGORY_NEGATIVE_EFFECTS_ON_CIVIC_DISCOURSE_OR_ELECTIONS": "Civic discourse / elections",
    "STATEMENT_CATEGORY_NON_CONSENSUAL_BEHAVIOUR": "Non-consensual behaviour",
    "STATEMENT_CATEGORY_PORNOGRAPHY_OR_SEXUALIZED_CONTENT": "Pornography / sexualized content",
    "STATEMENT_CATEGORY_PROTECTION_OF_MINORS": "Protection of minors",
    "STATEMENT_CATEGORY_RISK_FOR_PUBLIC_SECURITY": "Risk for public security",
    "STATEMENT_CATEGORY_SCAMS_AND_FRAUD": "Scams and fraud",
    "STATEMENT_CATEGORY_SELF_HARM": "Self-harm",
    "STATEMENT_CATEGORY_SCOPE_OF_PLATFORM_SERVICE": "Scope of platform service",
    "STATEMENT_CATEGORY_UNSAFE_AND_ILLEGAL_PRODUCTS": "Unsafe / illegal products",
    "STATEMENT_CATEGORY_VIOLENCE": "Violence"
}

category_select_sql = ",\n    ".join([
    f"SUM(CASE WHEN {col} = TRUE THEN count ELSE 0 END) AS \"{label}\""
    for col, label in statement_categories.items()
])

category_totals = con.execute(f"""
SELECT
    {category_select_sql}
FROM read_parquet('{path}')
WHERE
    (
        DECISION_VISIBILITY_CONTENT_REMOVED = TRUE
        OR DECISION_VISIBILITY_CONTENT_DISABLED = TRUE
    )
    AND application_date >= DATE '2026-01-01'
    AND application_date < DATE '2026-05-01'
""").fetchdf()

category_chart = category_totals.melt(
    var_name="statement_category",
    value_name="moderation_actions"
).sort_values(
    "moderation_actions",
    ascending=True
)

category_chart.to_csv(
    "output/statement_category_removed_content.csv",
    index=False
)

print(category_chart)


category_colours = [
    "#003865" if i % 2 == 0 else "#005EB8"
    for i in range(len(category_chart))
]

plt.figure(figsize=(5.8, 3.6))

plt.barh(
    category_chart["statement_category"],
    category_chart["moderation_actions"],
    color=category_colours
)

plt.xlabel("Removal / disable decisions")
plt.ylabel("Statement category")
plt.title("Statement Categories in Removed / Disabled Content")
plt.xscale("log")
plt.tight_layout()
plt.savefig("output/statement_category_removed_content.png", dpi=300)
plt.show()

# =========================================================
# 6a. Keywords for removed / disabled content (the keywords provide more granularity than the category, and content can be tagged with up to two keywords, so this gives a more detailed picture of the types of content removed/disabled)
# =========================================================

keywords = {
    "KEYWORD_ANIMAL_HARM": "Animal harm",
    "KEYWORD_ADULT_SEXUAL_MATERIAL": "Adult sexual material",
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS": "Age restrictions: minors",
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS": "Age restrictions",
    "KEYWORD_BIOMETRIC_DATA_BREACH": "Biometric data breach",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL": "Child sexual abuse material",
    "KEYWORD_CONTENT_PROMOTING_EATING_DISORDERS": "Eating disorders",
    "KEYWORD_COORDINATED_HARM": "Coordinated harm",
    "KEYWORD_COPYRIGHT_INFRINGEMENT": "Copyright infringement",
    "KEYWORD_DANGEROUS_TOYS": "Dangerous toys",
    "KEYWORD_DATA_FALSIFICATION": "Data falsification",
    "KEYWORD_DEFAMATION": "Defamation",
    "KEYWORD_DESIGN_INFRINGEMENT": "Design infringement",
    "KEYWORD_DISCRIMINATION": "Discrimination",
    "KEYWORD_DISINFORMATION": "Disinformation",
    "KEYWORD_FOREIGN_INFORMATION_MANIPULATION": "Foreign information manipulation",
    "KEYWORD_GENDER_BASED_VIOLENCE": "Gender-based violence",
    "KEYWORD_GEOGRAPHIC_INDICATIONS_INFRINGEMENT": "Geographic indications infringement",
    "KEYWORD_GEOGRAPHICAL_REQUIREMENTS": "Geographical requirements",
    "KEYWORD_GOODS_SERVICES_NOT_PERMITTED": "Goods/services not permitted",
    "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS": "Grooming / sexual enticement of minors",
    "KEYWORD_HATE_SPEECH": "Hate speech",
    "KEYWORD_HUMAN_EXPLOITATION": "Human exploitation",
    "KEYWORD_HUMAN_TRAFFICKING": "Human trafficking",
    "KEYWORD_ILLEGAL_ORGANIZATIONS": "Illegal organizations",
    "KEYWORD_IMAGE_BASED_SEXUAL_ABUSE": "Image-based sexual abuse",
    "KEYWORD_IMPERSONATION_ACCOUNT_HIJACKING": "Impersonation / account hijacking",
    "KEYWORD_INAUTHENTIC_ACCOUNTS": "Inauthentic accounts",
    "KEYWORD_INAUTHENTIC_LISTINGS": "Inauthentic listings",
    "KEYWORD_INAUTHENTIC_USER_REVIEWS": "Inauthentic user reviews",
    "KEYWORD_INCITEMENT_VIOLENCE_HATRED": "Incitement to violence / hatred",
    "KEYWORD_INSUFFICIENT_INFORMATION_TRADERS": "Insufficient trader information",
    "KEYWORD_LANGUAGE_REQUIREMENTS": "Language requirements",
    "KEYWORD_MISINFORMATION": "Misinformation",
    "KEYWORD_MISSING_PROCESSING_GROUND": "Missing processing ground",
    "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING": "Non-consensual image sharing",
    "KEYWORD_NON_CONSENSUAL_ITEMS_DEEPFAKE": "Non-consensual deepfake items",
    "KEYWORD_NUDITY": "Nudity",
    "KEYWORD_ONLINE_BULLYING_INTIMIDATION": "Online bullying / intimidation",
    "KEYWORD_PATENT_INFRINGEMENT": "Patent infringement",
    "KEYWORD_PHISHING": "Phishing",
    "KEYWORD_PYRAMID_SCHEMES": "Pyramid schemes",
    "KEYWORD_REGULATED_GOODS_SERVICES": "Regulated goods/services",
    "KEYWORD_RIGHT_TO_BE_FORGOTTEN": "Right to be forgotten",
    "KEYWORD_RISK_ENVIRONMENTAL_DAMAGE": "Environmental damage risk",
    "KEYWORD_RISK_PUBLIC_HEALTH": "Public health risk",
    "KEYWORD_SELF_MUTILATION": "Self-mutilation",
    "KEYWORD_STALKING": "Stalking",
    "KEYWORD_SUICIDE": "Suicide",
    "KEYWORD_TERRORIST_CONTENT": "Terrorist content",
    "KEYWORD_TRADE_SECRET_INFRINGEMENT": "Trade secret infringement",
    "KEYWORD_TRADEMARK_INFRINGEMENT": "Trademark infringement",
    "KEYWORD_UNLAWFUL_SALE_ANIMALS": "Unlawful sale of animals",
    "KEYWORD_UNSAFE_CHALLENGES": "Unsafe challenges",
    "KEYWORD_OTHER": "Other"
}

keyword_select_sql = ",\n    ".join([
    f"SUM(CASE WHEN {col} = TRUE THEN count ELSE 0 END) AS \"{label}\""
    for col, label in keywords.items()
])

keyword_totals = con.execute(f"""
SELECT
    {keyword_select_sql}
FROM read_parquet('{path}')
WHERE
    (
        DECISION_VISIBILITY_CONTENT_REMOVED = TRUE
        OR DECISION_VISIBILITY_CONTENT_DISABLED = TRUE
    )
    AND application_date >= DATE '2026-01-01'
    AND application_date < DATE '2026-05-01'
""").fetchdf()

keyword_chart = keyword_totals.melt(
    var_name="keyword",
    value_name="moderation_actions"
).sort_values(
    "moderation_actions",
    ascending=True
)

keyword_chart.to_csv(
    "output/keyword_removed_content.csv",
    index=False
)

print(keyword_chart)

keyword_colours = [
    "#003865" if i % 2 == 0 else "#005EB8"
    for i in range(len(keyword_chart))
]

plt.figure(figsize=(12, 14))

plt.barh(
    keyword_chart["keyword"],
    keyword_chart["moderation_actions"],
    color=keyword_colours
)

plt.xlabel("Removal / disable decisions")
plt.ylabel("Keyword")
plt.title("Keywords in Removed / Disabled Content")
plt.xscale("log")
plt.tight_layout()
plt.savefig("output/keyword_removed_content.png", dpi=300)
plt.show()

# =========================================================
# 6b. Top 15 keywords for removed / disabled content
# =========================================================

top_keywords_chart = (
    keyword_chart
    .sort_values("moderation_actions", ascending=False)
    .head(15)
    .sort_values("moderation_actions", ascending=False)
)

top_keywords_chart.to_csv(
    "output/top15_keyword_removed_content.csv",
    index=False
)

print(top_keywords_chart)

top_keyword_colours = [
    "#003865" if i % 2 == 0 else "#005EB8"
    for i in range(len(top_keywords_chart))
]

plt.figure(figsize=(6.2, 3.8))

plt.bar(
    top_keywords_chart["keyword"],
    top_keywords_chart["moderation_actions"],
    color=top_keyword_colours
)

plt.xlabel("Keyword")
plt.ylabel("Removal / disable decisions")
plt.title("Top 15 Keywords in Removed / Disabled Content")

plt.yscale("log")

plt.xticks(
    rotation=45,
    ha="right",
    fontsize=9
)

plt.subplots_adjust(
    left=0.12,
    right=0.98,
    top=0.88,
    bottom=0.35
)

plt.savefig(
    "output/top15_keyword_removed_content.png",
    dpi=300,
    bbox_inches="tight",
    pad_inches=0.05
)

plt.show()

# =========================================================
# 7. Source type of moderation decisions
# =========================================================

source_type_chart = con.execute(f"""
SELECT
    source_type,
    SUM(count) AS moderation_actions
FROM read_parquet('{path}')
WHERE
    application_date >= DATE '2026-01-01'
    AND application_date < DATE '2026-05-01'
    AND source_type IN (
        'SOURCE_VOLUNTARY',
        'SOURCE_TYPE_OTHER_NOTIFICATION',
        'SOURCE_ARTICLE_16',
        'SOURCE_TRUSTED_FLAGGER'
    )
GROUP BY source_type
ORDER BY moderation_actions ASC
""").fetchdf()

source_type_chart.to_csv(
    "output/source_type_decisions.csv",
    index=False
)

print(source_type_chart)

source_type_chart["source_label"] = source_type_chart["source_type"].replace({
    "SOURCE_VOLUNTARY": "Voluntary detection",
    "SOURCE_TYPE_OTHER_NOTIFICATION": "Other notification",
    "SOURCE_ARTICLE_16": "Article 16 notice",
    "SOURCE_TRUSTED_FLAGGER": "Trusted flagger"
})

source_colours = {
    "Voluntary detection": "#003865",
    "Other notification": "#005EB8",
    "Article 16 notice": "#008B8B",
    "Trusted flagger": "#C69214"
}

plt.figure(figsize=(5.8, 3.6))

plt.barh(
    source_type_chart["source_label"],
    source_type_chart["moderation_actions"],
    color=[
        source_colours.get(s, qmul_blue)
        for s in source_type_chart["source_label"]
    ]
)

plt.xlabel("Moderation decisions")
plt.ylabel("Source type")
plt.title("Source Type of Moderation Decisions")

plt.xscale("log")

plt.tight_layout()

plt.savefig(
    "output/source_type_decisions.png",
    dpi=300
)

plt.show()

