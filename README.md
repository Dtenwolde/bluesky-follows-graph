## Introduction
This is a little experimental dataset that analyses the Bluesky followers using DuckDB and DuckPGQ. 

DuckDB is an in-process analytical database that is quickly gaining popularity. 

[DuckPGQ](https://duckdb.org/community_extensions/extensions/duckpgq.html) is a community extension for DuckDB that makes it easier to work with graph workloads using the property graph data model. 
It supports the new SQL/PGQ standard which adds friendlier syntax for graph pattern matching and path-finding. 

In this case, we have the nodes stored in the `account` table, and the edges in `follows`. 
DuckPGQ then allows us to define a property graph over these tables as follows: 
```sql
CREATE OR REPLACE PROPERTY GRAPH bluesky
        VERTEX TABLES (account)
        EDGE TABLES (follows    SOURCE KEY (source) REFERENCES account (did)
                                DESTINATION KEY (destination) REFERENCES account (did)
)
```

Now we can use the new PGQ syntax. The following query counts the number of accounts up to 5 hops away from my account: 
```sql
SELECT count(*)
FROM GRAPH_TABLE (bluesky 
   MATCH (a:account WHERE a.handle = 'dtenwolde.bsky.social')
       -[f:follows]-> {,5}
       (b:account) 
   COLUMNS (
       a.displayName as source, 
       b.displayName as destination
   )
)
```

`()` are used to denote nodes, `[]` are used to denote edges. 
DuckPGQ supports pattern matching with various edge types: `-[]->`, `<-[]-`, `-[]-`, `<-[]->`


## Setup
```bash
virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run
```bash
python bluesky_analyse.py 
```

By default it uses the `bluesky.duckdb` database file, but if you want to point to a different file: 
```bash
python bluesky_analyse.py --dataset <another_db.duckdb>
```

If you want to scrape the Bluesky followers endpoint yourself (Can take a while though): 
```bash
python bluesky_analyse.py --scrape
```