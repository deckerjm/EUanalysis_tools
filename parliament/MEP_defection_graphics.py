
########
# Generate graphics from HowTheyVote MEP defection analysis
########

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


# -----------------------------
# 1. Load existing analysis outputs
# -----------------------------

output_dir = Path("output")
graphics_dir = output_dir / "graphics"

graphics_dir.mkdir(parents=True, exist_ok=True)

analysis_df = pd.read_csv(
    output_dir / "mep_votes_with_party_position.csv"
)

party_defection_rates = pd.read_csv(
    output_dir / "party_defection_rates.csv"
)

country_defection_rates = pd.read_csv(
    output_dir / "country_defection_rates.csv"
)


# -----------------------------
# 2. Remove duplicate MEP records
# -----------------------------

# Each MEP appears once per vote in the original dataset.
# For boxplots, we only need one observation per MEP.

mep_party_rates = (
    analysis_df[
        [
            "member_id",
            "member_group_short_label",
            "mep_party_defection_rate"
        ]
    ]
    .drop_duplicates(
        subset=["member_id", "member_group_short_label"]
    )
)

mep_country_rates = (
    analysis_df[
        [
            "member_id",
            "member_country_code",
            "mep_country_defection_rate"
        ]
    ]
    .drop_duplicates(
        subset=["member_id", "member_country_code"]
    )
)


# -----------------------------
# 3. Bar chart: Party defection rates
# -----------------------------

party_plot = party_defection_rates.sort_values(
    "party_defection_rate_pct",
    ascending=True
)

plt.figure(figsize=(12, 7))

plt.barh(
    party_plot["member_group_short_label"],
    party_plot["party_defection_rate_pct"]
)

plt.xlabel("Defection rate (%)")
plt.ylabel("Political group")
plt.title("MEP Defection Rates by Political Group")

plt.grid(axis="x", linestyle="--", alpha=0.5)
plt.tight_layout()

plt.savefig(
    graphics_dir / "party_defection_rates.png",
    dpi=300,
    bbox_inches="tight"
)

plt.show()
plt.close()


# -----------------------------
# 4. Bar chart: Country defection rates
# -----------------------------

country_plot = country_defection_rates.sort_values(
    "country_defection_rate_pct",
    ascending=True
)

plt.figure(figsize=(12, 9))

plt.barh(
    country_plot["member_country_code"],
    country_plot["country_defection_rate_pct"]
)

plt.xlabel("Defection rate (%)")
plt.ylabel("Country")
plt.title("MEP Defection Rates by Country")

plt.grid(axis="x", linestyle="--", alpha=0.5)
plt.tight_layout()

plt.savefig(
    graphics_dir / "country_defection_rates.png",
    dpi=300,
    bbox_inches="tight"
)

plt.show()
plt.close()


# -----------------------------
# 5. Boxplot: Individual MEP defection rates by party
# -----------------------------

party_order = (
    mep_party_rates
    .groupby("member_group_short_label")[
        "mep_party_defection_rate"
    ]
    .median()
    .sort_values()
    .index
)

party_boxplot_data = [
    mep_party_rates.loc[
        mep_party_rates["member_group_short_label"] == party,
        "mep_party_defection_rate"
    ].dropna()
    for party in party_order
]

plt.figure(figsize=(14, 8))

plt.boxplot(
    party_boxplot_data,
    tick_labels=party_order,
    showfliers=True
)

plt.xlabel("Political group")
plt.ylabel("Individual MEP defection rate (%)")
plt.title("Distribution of Individual MEP Defection Rates by Political Group")

plt.xticks(rotation=45, ha="right")
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()

plt.savefig(
    graphics_dir / "party_defection_boxplot.png",
    dpi=300,
    bbox_inches="tight"
)

plt.show()
plt.close()


# -----------------------------
# 6. Boxplot: Individual MEP defection rates by country
# -----------------------------

country_order = (
    mep_country_rates
    .groupby("member_country_code")[
        "mep_country_defection_rate"
    ]
    .median()
    .sort_values()
    .index
)

country_boxplot_data = [
    mep_country_rates.loc[
        mep_country_rates["member_country_code"] == country,
        "mep_country_defection_rate"
    ].dropna()
    for country in country_order
]

plt.figure(figsize=(15, 9))

plt.boxplot(
    country_boxplot_data,
    tick_labels=country_order,
    showfliers=True
)

plt.xlabel("Country")
plt.ylabel("Individual MEP defection rate (%)")
plt.title("Distribution of Individual MEP Defection Rates by Country")

plt.xticks(rotation=45, ha="right")
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()

plt.savefig(
    graphics_dir / "country_defection_boxplot.png",
    dpi=300,
    bbox_inches="tight"
)

plt.show()
plt.close()


# -----------------------------
# 7. Completion message
# -----------------------------

print("\nAll graphics have been generated.")
print(f"Graphics saved in: {graphics_dir}")

print("\nGenerated files:")
print("- party_defection_rates.png")
print("- country_defection_rates.png")
print("- party_defection_boxplot.png")
print("- country_defection_boxplot.png")
