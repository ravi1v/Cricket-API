import sqlite3

DB_NAME = "players.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        country TEXT,
        image_url TEXT,
        role TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS batting_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        format TEXT,
        matches INTEGER,
        runs INTEGER,
        highest_score TEXT,
        average REAL,
        strike_rate REAL,
        hundreds INTEGER,
        fifties INTEGER,
        FOREIGN KEY (player_id) REFERENCES players(id)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bowling_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        format TEXT,
        balls INTEGER,
        runs INTEGER,
        wickets INTEGER,
        best_bowling_innings TEXT,
        economy REAL,
        five_wickets INTEGER,
        FOREIGN KEY (player_id) REFERENCES players(id)
    );
    """)
    conn.commit()
    conn.close()

def insert_player(player_data):
    """
    player_data is expected in this structure:
    {
        "name": str,
        "country": str,
        "image": str,
        "role": str,
        "batting_stats": {
            format_name: {
                "matches": int,
                "runs": int,
                "highest_score": str,
                "average": float,
                "strike_rate": float,
                "hundreds": int,
                "fifties": int,
            },
            ...
        },
        "bowling_stats": {
            format_name: {
                "balls": int,
                "runs": int,
                "wickets": int,
                "best_bowling_innings": str,
                "economy": float,
                "five_wickets": int,
            },
            ...
        }
    }
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Insert player basic info
    cursor.execute("""
        INSERT OR IGNORE INTO players (name, country, image_url, role)
        VALUES (?, ?, ?, ?)
    """, (player_data["name"], player_data["country"], player_data["image"], player_data["role"]))
    conn.commit()

    # Get player id
    cursor.execute("SELECT id FROM players WHERE name = ?", (player_data["name"],))
    player_id = cursor.fetchone()[0]

    # Insert batting stats
    for fmt, stats in player_data["batting_stats"].items():
        cursor.execute("""
            INSERT INTO batting_stats
            (player_id, format, matches, runs, highest_score, average, strike_rate, hundreds, fifties)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            player_id, fmt,
            int(stats["matches"]) if stats["matches"].isdigit() else 0,
            int(stats["runs"]) if stats["runs"].isdigit() else 0,
            stats["highest_score"],
            float(stats["average"]) if is_float(stats["average"]) else 0,
            float(stats["strike_rate"]) if is_float(stats["strike_rate"]) else 0,
            int(stats["hundreds"]) if stats["hundreds"].isdigit() else 0,
            int(stats["fifties"]) if stats["fifties"].isdigit() else 0
        ))

    # Insert bowling stats
    for fmt, stats in player_data["bowling_stats"].items():
        cursor.execute("""
            INSERT INTO bowling_stats
            (player_id, format, balls, runs, wickets, best_bowling_innings, economy, five_wickets)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            player_id, fmt,
            int(stats["balls"]) if stats["balls"].isdigit() else 0,
            int(stats["runs"]) if stats["runs"].isdigit() else 0,
            int(stats["wickets"]) if stats["wickets"].isdigit() else 0,
            stats["best_bowling_innings"],
            float(stats["economy"]) if is_float(stats["economy"]) else 0,
            int(stats["five_wickets"]) if stats["five_wickets"].isdigit() else 0
        ))

    conn.commit()
    conn.close()

def is_float(value):
    try:
        float(value)
        return True
    except:
        return False

def get_player_by_name(player_name):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM players WHERE name = ?", (player_name,))
    player = cursor.fetchone()
    if not player:
        conn.close()
        return None

    player_id = player[0]
    cursor.execute("SELECT * FROM batting_stats WHERE player_id = ?", (player_id,))
    batting_stats = cursor.fetchall()

    cursor.execute("SELECT * FROM bowling_stats WHERE player_id = ?", (player_id,))
    bowling_stats = cursor.fetchall()

    conn.close()

    return {
        "player": player,
        "batting_stats": batting_stats,
        "bowling_stats": bowling_stats
    }
if __name__ == "__main__":
    create_tables()
    print("Database tables created successfully.")
