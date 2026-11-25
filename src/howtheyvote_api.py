
import requests
import pandas as pd
import time

BASE_URL = "https://howtheyvote.eu/api"

# API request settings
REQUEST_INTERVAL = 2.0  # Minimum seconds between requests
MAX_RETRIES = 6
REQUEST_TIMEOUT = 30

last_request_time = 0


############
# Shared API request function with rate limiting
############

def api_request(url, params=None):

    global last_request_time

    for attempt in range(MAX_RETRIES):

        # Enforce minimum interval between requests
        elapsed = time.monotonic() - last_request_time
        wait = REQUEST_INTERVAL - elapsed

        if wait > 0:
            time.sleep(wait)

        response = requests.get(
            url,
            headers={"Accept": "application/json"},
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        last_request_time = time.monotonic()

        # Handle rate limiting (HTTP 429)
        if response.status_code == 429:

            retry_after = response.headers.get("Retry-After")

            try:
                wait = float(retry_after) if retry_after else 60
            except ValueError:
                wait = 60

            print(
                f"Rate limit reached. "
                f"Waiting {wait} seconds..."
            )

            time.sleep(wait)
            continue

        response.raise_for_status()

        return response.json()

    raise RuntimeError(
        "Maximum retries exceeded due to API rate limiting."
    )


############
# Function to get list of votes by search parameters
# Includes automatic pagination
############

def get_list(date=None, date_gte=None, date_lte=None, facets=None):

    url = f"{BASE_URL}/votes"

    params = {
        "date": date,
        "date[gte]": date_gte,
        "date[lte]": date_lte,
        "facets": facets
    }

    # Remove parameters that haven't been specified
    params = {
        key: value
        for key, value in params.items()
        if value is not None
    }

    all_results = []
    page = 0
    data = {}

    while True:

        # Request the current page
        page_params = params.copy()
        page_params["page"] = page

        data = api_request(
            url,
            params=page_params
        )

        # Add results from this page
        all_results.extend(data["results"])

        print(
            f"Retrieved page {page + 1}: "
            f"{len(data['results'])} votes"
        )

        # Stop when there are no more pages
        if not data["has_next"]:
            break

        page += 1

    # Replace results with the complete collection
    data["results"] = all_results

    print(
        f"Total votes retrieved: {len(all_results)}"
    )

    return data


##############
# Function to get results for an individual vote
##############

def get_vote(vote_id):

    url = f"{BASE_URL}/votes/{vote_id}"

    return api_request(url)


##############
# Function to process an individual vote
##############

def process_vote(vote_id):

    # Retrieve API response
    vote = get_vote(vote_id)

    # 1. Vote metadata
    metadata = {
        k: v for k, v in vote.items()
        if k not in [
            "member_votes",
            "stats",
            "groups_required_for_majority",
            "related"
        ]
    }

    # Add overall voting statistics
    metadata.update({
        f"total_{k.lower()}": v
        for k, v in vote["stats"]["total"].items()
    })

    df_vote = pd.json_normalize(
        metadata,
        sep="_"
    )

    # 2. Individual MEP votes
    df_members = pd.json_normalize(
        vote["member_votes"],
        sep="_"
    )

    # 3. Political group statistics
    df_groups = pd.json_normalize(
        vote["stats"]["by_group"],
        sep="_"
    )

    # 4. Country statistics
    df_countries = pd.json_normalize(
        vote["stats"]["by_country"],
        sep="_"
    )

    # Add vote ID to the individual datasets
    df_members["vote_id"] = vote_id
    df_groups["vote_id"] = vote_id
    df_countries["vote_id"] = vote_id

    return {
        "vote": df_vote,
        "members": df_members,
        "groups": df_groups,
        "countries": df_countries
    }


##############
# Function to retrieve multiple votes and combine
# vote, MEP, political group and country information
##############

def get_votes_for_list(date=None, date_gte=None, date_lte=None, facets=None):

    # Step 1: Retrieve all votes matching search parameters
    vote_list = get_list(
        date=date,
        date_gte=date_gte,
        date_lte=date_lte,
        facets=facets
    )

    all_member_votes = []

    # Step 2: Process each individual vote
    for i, vote in enumerate(vote_list["results"], start=1):

        vote_id = vote["id"]

        print(
            f"Processing vote {i}/{len(vote_list['results'])}: "
            f"{vote_id}"
        )

        vote_data = process_vote(vote_id)

        # Extract the four DataFrames
        df_vote = vote_data["vote"].copy()
        df_members = vote_data["members"].copy()
        df_groups = vote_data["groups"].copy()
        df_countries = vote_data["countries"].copy()

        # Step 3: Add vote-wide information to every MEP row
        df_vote_repeated = pd.concat(
            [df_vote] * len(df_members),
            ignore_index=True
        )

        df_combined = pd.concat(
            [
                df_vote_repeated,
                df_members.reset_index(drop=True)
            ],
            axis=1
        )

        # Step 4: Add political group statistics
        df_groups = df_groups.rename(columns={
            "group_code": "member_group_code",
            "stats_FOR": "group_FOR",
            "stats_AGAINST": "group_AGAINST",
            "stats_ABSTENTION": "group_ABSTENTION",
            "stats_DID_NOT_VOTE": "group_DID_NOT_VOTE"
        })

        # Merge group statistics using political group code
        df_combined = df_combined.merge(
            df_groups,
            on="member_group_code",
            how="left",
            validate="many_to_one"
        )

        # Step 5: Add country statistics
        df_countries = df_countries.rename(columns={
            "country_code": "member_country_code",
            "stats_FOR": "country_FOR",
            "stats_AGAINST": "country_AGAINST",
            "stats_ABSTENTION": "country_ABSTENTION",
            "stats_DID_NOT_VOTE": "country_DID_NOT_VOTE"
        })

        # Merge country statistics using country code
        df_combined = df_combined.merge(
            df_countries,
            on="member_country_code",
            how="left",
            validate="many_to_one"
        )

        # Step 6: Store combined MEP-level data
        all_member_votes.append(df_combined)

    # Step 7: Combine all votes into one DataFrame
    if all_member_votes:

        df_all_votes = pd.concat(
            all_member_votes,
            ignore_index=True
        )

    else:
        df_all_votes = pd.DataFrame()

    return df_all_votes

