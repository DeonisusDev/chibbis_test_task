import sqlite3
import logging
import sys
import json
from pathlib import Path

import requests

# Config

BASE_URL = "https://jsonplaceholder.typicode.com"
DB_PATH = Path("data.db")

ENDPOINTS = {
    "users":    "/users",
    "posts":    "/posts",
    "comments": "/comments",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# Database

DDL = """
CREATE TABLE IF NOT EXISTS users (
    id       INTEGER PRIMARY KEY,
    name     TEXT    NOT NULL,
    username TEXT    NOT NULL,
    email    TEXT    NOT NULL,
    phone    TEXT,
    website  TEXT,
    company  TEXT,
    address  TEXT
);

CREATE TABLE IF NOT EXISTS posts (
    id      INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title   TEXT    NOT NULL,
    body    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS comments (
    id      INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL REFERENCES posts(id),
    name    TEXT    NOT NULL,
    email   TEXT    NOT NULL,
    body    TEXT    NOT NULL
);
"""

UPSERT = {
    "users": """
        INSERT INTO users (id, name, username, email, phone, website, company, address)
        VALUES (:id, :name, :username, :email, :phone, :website, :company, :address)
        ON CONFLICT(id) DO UPDATE SET
            name     = excluded.name,
            username = excluded.username,
            email    = excluded.email,
            phone    = excluded.phone,
            website  = excluded.website,
            company  = excluded.company,
            address  = excluded.address
    """,
    "posts": """
        INSERT INTO posts (id, user_id, title, body)
        VALUES (:id, :user_id, :title, :body)
        ON CONFLICT(id) DO UPDATE SET
            user_id = excluded.user_id,
            title   = excluded.title,
            body    = excluded.body
    """,
    "comments": """
        INSERT INTO comments (id, post_id, name, email, body)
        VALUES (:id, :post_id, :name, :email, :body)
        ON CONFLICT(id) DO UPDATE SET
            post_id = excluded.post_id,
            name    = excluded.name,
            email   = excluded.email,
            body    = excluded.body
    """,
}


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()
    log.info("Database schema ready.")


# Fetch

def fetch(endpoint: str) -> list[dict]:
    url = BASE_URL + endpoint
    log.info("Fetching %s ...", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    log.info("  -> %d records received.", len(data))
    return data


# Transform

def transform_users(raw: list[dict]) -> list[dict]:
    return [
        {
            "id":       r["id"],
            "name":     r["name"],
            "username": r["username"],
            "email":    r["email"],
            "phone":    r.get("phone"),
            "website":  r.get("website"),
            # Flatten nested objects to JSON strings for simplicity
            "company":  json.dumps(r.get("company", {}), ensure_ascii=False),
            "address":  json.dumps(r.get("address", {}), ensure_ascii=False),
        }
        for r in raw
    ]


def transform_posts(raw: list[dict]) -> list[dict]:
    return [
        {
            "id":      r["id"],
            "user_id": r["userId"],
            "title":   r["title"],
            "body":    r["body"],
        }
        for r in raw
    ]


def transform_comments(raw: list[dict]) -> list[dict]:
    return [
        {
            "id":      r["id"],
            "post_id": r["postId"],
            "name":    r["name"],
            "email":   r["email"],
            "body":    r["body"],
        }
        for r in raw
    ]


TRANSFORMERS = {
    "users":    transform_users,
    "posts":    transform_posts,
    "comments": transform_comments,
}


# Load

def load(conn: sqlite3.Connection, table: str, rows: list[dict]) -> None:
    conn.executemany(UPSERT[table], rows)
    conn.commit()
    log.info("  -> %d rows upserted into '%s'.", len(rows), table)


# Pipeline

def run() -> None:
    log.info("Starting ETL pipeline.")

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        init_db(conn)

        # Load in dependency order: users -> posts -> comments
        for table in ("users", "posts", "comments"):
            raw = fetch(ENDPOINTS[table])
            rows = TRANSFORMERS[table](raw)
            load(conn, table, rows)

    log.info("ETL pipeline finished. Database: %s", DB_PATH.resolve())


if __name__ == "__main__":
    run()
