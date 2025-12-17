from yoyo import step

steps = [
    step("""
    CREATE TABLE IF NOT EXISTS sheets_users (
        id INTEGER,
        user_country VARCHAR(1000),
        user_type VARCHAR(100),
        user_type_id INTEGER,
        job_title VARCHAR(100)
    );
    """, """
    DROP TABLE IF EXISTS sheets_users;
    """)
]
