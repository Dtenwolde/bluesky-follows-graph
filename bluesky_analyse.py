import duckdb
import argparse

def display_stats(conn):
    final_stats = conn.execute("SELECT COUNT(*) FROM account").fetchone()[0]
    print(f"Total accounts in the database: {final_stats}")
    final_follow_stats = conn.execute("SELECT COUNT(*) FROM follows").fetchone()[0]
    print(f"Total follows in the database: {final_follow_stats}")

    print(conn.execute(
        "FROM GRAPH_TABLE (bluesky "
        "   MATCH (a:account WHERE a.handle = 'dtenwolde.bsky.social')"
        "       -[f:follows]->"
        "       (b:account) "
        "   COLUMNS ("
        "       a.displayName as source, "
        "       b.displayName as destination"
        "   )"
        ")"
    ).fetchdf())

    print(conn.execute(
        "SELECT count(*)"
        "FROM GRAPH_TABLE (bluesky "
        "   MATCH (a:account WHERE a.handle = 'dtenwolde.bsky.social')"
        "       -[f:follows]-> {,5}"
        "       (b:account) "
        "   COLUMNS ("
        "       a.displayName as source, "
        "       b.displayName as destination"
        "   )"
        ")"
    ).fetchdf())


def main():
    parser = argparse.ArgumentParser(description="Scrape Bluesky data or display existing statistics.")
    parser.add_argument('--dataset', type=str, default="bluesky.duckdb", help="Path to the DuckDB file")
    args = parser.parse_args()

    # Connect to the specified DuckDB file
    conn = duckdb.connect(args.dataset)
    conn.execute("INSTALL duckpgq from community")
    conn.execute("LOAD duckpgq")

    conn.execute("""CREATE OR REPLACE PROPERTY GRAPH bluesky
        VERTEX TABLES (account)
        EDGE TABLES (follows    SOURCE KEY (source) REFERENCES account (did)
                                DESTINATION KEY (destination) REFERENCES account (did)
        )  """)

    display_stats(conn)

if __name__ == '__main__':
    main()