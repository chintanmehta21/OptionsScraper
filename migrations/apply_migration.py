"""Apply SQL migration to Supabase Postgres via direct connection."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import psycopg2


def get_connection_uri():
    uri = os.environ.get("SUPABASE_DB_URI")
    if uri:
        return uri
    # Construct from parts using session-mode pooler (IPv4, supports DDL)
    project_ref = "tksnankwlpqhalqsofon"
    password = os.environ.get("SUPABASE_DB_PASSWORD")
    if not password:
        print("ERROR: SUPABASE_DB_URI or SUPABASE_DB_PASSWORD must be set")
        sys.exit(1)
    return f"postgresql://postgres.{project_ref}:{password}@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"


def apply_migration(sql_file: str):
    uri = get_connection_uri()
    print(f"Connecting to Supabase Postgres...")

    conn = psycopg2.connect(uri)
    conn.autocommit = True
    cur = conn.cursor()

    with open(sql_file) as f:
        sql = f.read()

    print(f"Executing migration from {sql_file}...")
    try:
        cur.execute(sql)
        print("Migration applied successfully.")
    except Exception as e:
        print(f"Migration error: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    # Verify tables
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in cur.fetchall()]
    print(f"\nTables in public schema: {tables}")

    cur.execute("""
        SELECT table_name FROM information_schema.views
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    views = [row[0] for row in cur.fetchall()]
    print(f"Views in public schema: {views}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    sql_file = os.path.join(os.path.dirname(__file__), "001_initial_schema.sql")
    apply_migration(sql_file)
