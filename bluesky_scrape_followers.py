import duckdb
import requests
import pandas as pd
import argparse

def get_all_followers(actor, limit=100):
    url = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows"
    followers = []
    cursor = None

    while True:
        params = {
            'actor': actor,
            'limit': limit
        }
        if cursor:
            params['cursor'] = cursor

        response = requests.get(url, params=params)
        data = response.json()

        # Append new followers to the list
        followers.extend(data.get('follows', []))

        # Check if there's a next page
        cursor = data.get('cursor')
        if not cursor:
            break

    # Convert list of followers to a DataFrame
    df = pd.DataFrame(followers)
    return df

def scrape_data(conn):
    # DDL for table creation (without dropping tables)
    ddl = """
    CREATE OR REPLACE TABLE account (did VARCHAR UNIQUE NOT NULL, handle VARCHAR, displayName VARCHAR);
    CREATE OR REPLACE TABLE follows (source VARCHAR, destination VARCHAR);
    """
    conn.execute(ddl)

    # Insert starting data and scrape followers
    actor = "did:plc:7qqkrwwec4qeujs6hthlgpbe"  # Replace with DID or handle
    all_followers = get_all_followers(actor)

    conn.execute(f"""
        INSERT OR IGNORE INTO account 
        SELECT did, handle, displayName FROM all_followers;
    """)

    all_accounts = conn.execute("SELECT did FROM account").fetchall()
    accounts_handled = set()

    for iteration in range(1, 3):
        print(f"Starting iteration {iteration}...")

        for index, account in enumerate(all_accounts, start=1):
            did = account[0]
            if did in accounts_handled:
                continue

            all_followers = get_all_followers(did)
            num_followers = len(all_followers)

            if num_followers == 0:
                print(f"Account {did} has no followers.")
                continue

            query = f"""
            CREATE OR REPLACE TEMP TABLE unnested_feed AS 
            SELECT did, handle, displayname FROM all_followers;

            INSERT INTO account (did, handle, displayName)
            (SELECT DISTINCT unnested_feed.did, 
                             unnested_feed.handle AS handle, 
                             unnested_feed.displayName AS display_name
             FROM unnested_feed)
            ON CONFLICT (did) DO NOTHING;
            """

            follows_query = f"""
            INSERT INTO follows (source, destination)
            SELECT '{did}' AS source, unnested_feed.did AS destination
            FROM unnested_feed
            LEFT JOIN follows ON follows.source = '{did}' AND follows.destination = unnested_feed.did
            WHERE follows.source IS NULL;
            """

            conn.execute(query)
            conn.execute(follows_query)

            accounts_handled.add(did)
            print(f"[{iteration}] Processed account {did}: Added {num_followers} followers.")

            if index % 10 == 0:
                num_accounts = conn.execute("SELECT COUNT(*) FROM account").fetchone()[0]
                num_follows = conn.execute("SELECT COUNT(*) FROM follows").fetchone()[0]
                print(f"[{iteration}] Progress: {index}/{len(all_accounts)} accounts processed.")
                print(f"Current totals - Accounts: {num_accounts}, Follows: {num_follows}")

        all_accounts = conn.execute("SELECT did FROM account").fetchall()

    print("Finished processing all iterations.")
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, default="bluesky.duckdb", help="Path to the DuckDB file")
    args = parser.parse_args()

    # Connect to the specified DuckDB file
    conn = duckdb.connect(args.dataset)

    scrape_data(conn)


if __name__ == '__main__':
    main()