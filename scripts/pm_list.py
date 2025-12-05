# scripts/pm_list.py
from __future__ import annotations
import os
import sqlite3

# Path to your *static lists* DB (relative to repo root)
DB_PATH = os.path.join("data", "static_lists.sqlite3")

SQL = """
CREATE TABLE IF NOT EXISTS pm_list (
    "MAT" TEXT PRIMARY KEY,
    "Program Manager" TEXT,
    "LAN ID" TEXT
);

INSERT OR REPLACE INTO pm_list ("MAT", "Program Manager", "LAN ID") VALUES
    ('49B', 'Alwin Delapena', 'a5d9@pge.com'),
    ('49C', 'Alwin Delapena', 'a5d9@pge.com'),
    ('49D', 'Alwin Delapena', 'a5d9@pge.com'),
    ('49E', 'Alwin Delapena', 'a5d9@pge.com'),
    ('49S', 'Alwin Delapena', 'a5d9@pge.com'),
    ('49X', 'Alwin Delapena', 'a5d9@pge.com'),
    ('3UT', 'Tee Lin', 't2l2@pge.com'),
    ('49H', 'Tee Lin', 't2l2@pge.com'),
    ('3UP', 'Tee Lin', 't2l2@pge.com'),
    ('3UC', 'Tee Lin', 't2l2@pge.com'),
    ('49M', 'Tee Lin', 't2l2@pge.com'),
    ('3UA', 'Tee Lin', 't2l2@pge.com'),
    ('56S', 'Tee Lin', 't2l2@pge.com'),
    ('06B', 'Swaran Bhatthal', 's4bw@pge.com'),
    ('48L', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06A', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06D', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06E', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06G', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06H', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06L', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06M', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06N', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06O', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06P', 'Norah Jamaly', 'n1jl@pge.com'),
    ('06S', 'Norah Jamaly', 'n1jl@pge.com'),
    ('56T', 'Raghu Rao', 'r5r4@pge.com'),
    ('2AE', 'Kaitlyn Hanley', 'k3m1@pge.com'),
    ('2BD', 'Kaitlyn Hanley', 'k3m1@pge.com'),
    ('3UL', 'Raghu Rao', 'r5r4@pge.com'),
    ('KAF', 'Kaitlyn Hanley', 'k3m1@pge.com'),
    ('07C', 'Stephanie Juarez', 's8b2@pge.com'),
    ('07D', 'Stephanie Juarez', 's8b2@pge.com'),
    ('07O', 'Stephanie Juarez', 's8b2@pge.com');
"""

def main() -> None:
    # Ensure data/ exists
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.executescript(SQL)
        conn.commit()
        print(f"pm_list table created/updated successfully in {DB_PATH!r}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
