# scripts/ds73_contact_list.py
from __future__ import annotations
import os
import sqlite3

# Path to your *static lists* DB (relative to repo root)
DB_PATH = os.path.join("data", "static_lists.sqlite3")

SQL = """
CREATE TABLE IF NOT EXISTS ds73_contact_list (
    "Div" TEXT PRIMARY KEY,
    "DS73 Contact" TEXT,
    "LAN ID" TEXT
);

INSERT OR REPLACE INTO ds73_contact_list ("Div", "DS73 Contact", "LAN ID") VALUES
    ('CC', 'Adrianne Thoroddsson-Weathers', 'act0@pge.com'),
    ('LP', 'Adrianne Thoroddsson-Weathers', 'act0@pge.com'),
    ('SA', 'Alex Ly', 'adlk@pge.com'),
    ('NB', 'Alex Ly', 'adlk@pge.com'),
    ('ST', 'Alex Ly', 'adlk@pge.com'),
    ('FR', 'Vincent Rodriguez', 'vxro@pge.com'),
    ('YO', 'Nick Gargano', 'ncg5@pge.com'),
    ('PN', 'Glenn Esguerra', 'gae9@pge.com'),
    ('SF', 'Glenn Esguerra', 'gae9@pge.com'),
    ('HM', 'Nick Morelli', 'nmmb@pge.com'),
    ('SO', 'Nick Morelli', 'nmmb@pge.com'),
    ('DA', 'Ivan Baez', 'irb2@pge.com'),
    ('SJ', 'Ivan Baez', 'irb2@pge.com'),
    ('DI', 'Osami Takeshima', 'oxt1@pge.com'),
    ('EB', 'Osami Takeshima', 'oxt1@pge.com'),
    ('SI', 'Arnaldi Rustandi', 'airb@pge.com'),
    ('NV', 'Arnaldi Rustandi', 'airb@pge.com'),
    ('MI', 'Edward Miltimore Jr.', 'eamw@pge.com'),
    ('HB', 'Nick Morelli', 'nmmb@pge.com');
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
        print(f"ds73_contact_list table created/updated successfully in {DB_PATH!r}.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
