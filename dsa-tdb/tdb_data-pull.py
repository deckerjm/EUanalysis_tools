## IMPORTANT 
# Data should be pulled using the following command-line command: 
# dsa-tdb-cli download-aggs -o data/tdb/ -i 2026-05-01 -f 2026-05-13 
# (replace dates with your desired range)

import duckdb
import pandas as pd

con = duckdb.connect()

path = "data/tdb/aggregated-complete.parquet"

# create overview of entire dataset with column names, types, distinct values, null counts, and sample values
schema = con.execute(f"""
DESCRIBE
SELECT *
FROM read_parquet('{path}')
""").fetchdf()

results = []

for col in schema["column_name"]:

    print(f"Profiling: {col}")

    try:

        query = f"""
        SELECT
            COUNT(*) AS rows,
            APPROX_COUNT_DISTINCT("{col}") AS distinct_values,
            SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS nulls
        FROM (
            SELECT *
            FROM read_parquet('{path}')
            LIMIT 500000
        )
        """

        stats = con.execute(query).fetchone()

        sample_query = f"""
        SELECT DISTINCT "{col}"
        FROM read_parquet('{path}')
        WHERE "{col}" IS NOT NULL
        LIMIT 5
        """

        samples = con.execute(sample_query).fetchdf()[col].tolist()

        results.append({
            "column": col,
            "type": schema.loc[
                schema["column_name"] == col,
                "column_type"
            ].values[0],
            "approx_distinct": stats[1],
            "nulls": stats[2],
            "sample_values": str(samples)
        })

    except Exception as e:

        results.append({
            "column": col,
            "error": str(e)
        })

profile = pd.DataFrame(results)

profile.to_csv("dsa_column_profile.csv", index=False)

print(profile.head())

print("\nSaved to dsa_column_profile.csv")

# Fetch column values for 'decision_visibility' column

