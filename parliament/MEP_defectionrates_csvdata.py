########
# use csv data from howtheyvote.eu to analyze MEP defection rates. 
########

#import necessary libaries
import pandas as pd
from pathlib import Path


# -----------------------------
# 1. Load and combine CSV files
# -----------------------------

data_dir = Path("data/individualvotes")  # change if needed

csv_files = list(data_dir.glob("*_members.csv"))

if not csv_files:
    raise FileNotFoundError(f"No *_members.csv files found in: {data_dir.resolve()}")

dfs = []

for file in csv_files:
    vote_id = int(file.stem.split("_")[0])

    df = pd.read_csv(file)
    df["vote_id"] = vote_id
    df["source_file"] = file.name

    dfs.append(df)

all_votes = pd.concat(dfs, ignore_index=True)

# -----------------------------
# 2. Optional: exclude non-votes
# -----------------------------

analysis_df = all_votes[
    all_votes["position"] != "VotePosition.DID_NOT_VOTE"
].copy()

# -----------------------------
# 3. Calculate party majority position
# -----------------------------

party_vote_counts = (
    analysis_df
    .groupby(["vote_id", "member.group.short_label", "position"])
    .size()
    .reset_index(name="n")
)

party_majority = (
    party_vote_counts
    .sort_values(
        ["vote_id", "member.group.short_label", "n"],
        ascending=[True, True, False]
    )
    .drop_duplicates(
        subset=["vote_id", "member.group.short_label"]
    )
    .rename(columns={"position": "party.pos"})
)

# -----------------------------
# 4. Append party.pos to each MEP
# -----------------------------

analysis_df = analysis_df.merge(
    party_majority[
        ["vote_id", "member.group.short_label", "party.pos"]
    ],
    on=["vote_id", "member.group.short_label"],
    how="left"
)

# -----------------------------
# 5. Calculate defection indicator
# -----------------------------

analysis_df["defected"] = (
    analysis_df["position"] != analysis_df["party.pos"]
)

# -----------------------------
# 6. Calculate defection rate by party
# -----------------------------

party_defection_rates = (
    analysis_df
    .groupby("member.group.short_label")
    .agg(
        total_votes=("defected", "size"),
        defections=("defected", "sum"),
        defection_rate=("defected", "mean")
    )
    .reset_index()
)

party_defection_rates["defection_rate_pct"] = (
    party_defection_rates["defection_rate"] * 100
)

party_defection_rates = party_defection_rates.sort_values(
    "defection_rate_pct",
    ascending=False
)

# -----------------------------
# 7. Save outputs
# -----------------------------

analysis_df.to_csv(
    "output/mep_votes_with_party_position.csv",
    index=False
)

party_defection_rates.to_csv(
    "output/party_defection_rates.csv",
    index=False
)

# -----------------------------
# 8. View results
# -----------------------------

print("Combined vote-level dataset:")
print(analysis_df.head())

print("\nParty defection rates:")
print(party_defection_rates)

