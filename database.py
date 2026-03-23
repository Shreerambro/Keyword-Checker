import sqlite3
import datetime
import os

DB_FILE = os.path.join(os.path.dirname(__file__), "bot_data.db")

def _get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = _get_conn()
    c = conn.cursor()
    # Admins
    c.execute('''CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)''')
    # Searches
    c.execute('''CREATE TABLE IF NOT EXISTS searches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        keywords TEXT,
        source TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )''')
    # Results
    c.execute('''CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        search_id INTEGER,
        keyword TEXT,
        result_line TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(keyword, result_line)
    )''')
    conn.commit()
    conn.close()

init_db()

# Admin Management
def get_admins():
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT user_id FROM admins")
    admins = [row['user_id'] for row in c.fetchall()]
    conn.close()
    return admins

def add_admin(user_id):
    conn = _get_conn()
    c = conn.cursor()
    try:
        c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()

def remove_admin(user_id):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    return True

# Search Management
def create_search(user_id, search_terms, source):
    conn = _get_conn()
    c = conn.cursor()
    keywords = ", ".join(search_terms)
    c.execute("INSERT INTO searches (user_id, keywords, source) VALUES (?, ?, ?)", 
              (user_id, keywords, source))
    search_id = c.lastrowid
    conn.commit()
    conn.close()
    return search_id

def save_result(search_id, line, term):
    conn = _get_conn()
    c = conn.cursor()
    try:
        c.execute("INSERT OR IGNORE INTO results (search_id, keyword, result_line) VALUES (?, ?, ?)", 
                  (search_id, term, line))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

def get_total_stats():
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(id) FROM searches")
    total_searches = c.fetchone()[0]
    
    c.execute("SELECT COUNT(DISTINCT keyword) FROM results")
    total_keywords = c.fetchone()[0]
    
    c.execute("SELECT COUNT(id) FROM results")
    total_unique_results = c.fetchone()[0]
    
    # Just returning total unique as total_hits since we use IGNORE for duplicates
    # If we want exact hits we'd need another table, but this is fine for now
    total_hits = total_unique_results
    conn.close()
    
    return {
        'total_searches': total_searches,
        'total_keywords': total_keywords,
        'total_unique_results': total_unique_results,
        'total_hits': total_hits
    }

def get_keyword_stats(limit=15):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT keyword, COUNT(id) as total, MAX(timestamp) as last_searched
        FROM results
        GROUP BY keyword
        ORDER BY total DESC
        LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    
    return [{
        'keyword': r['keyword'],
        'total_hits': r['total'],
        'unique_hits': r['total'], # since we unique them in db
        'last_searched': r['last_searched']
    } for r in rows]

def get_recent_searches(limit=10):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT user_id, keywords, source, timestamp FROM searches ORDER BY id DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    
    return [{
        'user_id': r['user_id'],
        'keywords': r['keywords'],
        'source': r['source'],
        'timestamp': r['timestamp']
    } for r in rows]

def get_all_keywords_list():
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT DISTINCT keyword FROM results ORDER BY keyword ASC")
    rows = c.fetchall()
    conn.close()
    return [r['keyword'] for r in rows]

def get_results_by_keyword(keyword, limit=50000, unique_only=True):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT result_line, timestamp FROM results WHERE keyword = ? ORDER BY id DESC LIMIT ?", (keyword, limit))
    rows = c.fetchall()
    conn.close()
    return [{
        'result_line': r['result_line'],
        'timestamp': r['timestamp']
    } for r in rows]

def import_results(keyword, results, user_id):
    conn = _get_conn()
    c = conn.cursor()
    imported = 0
    skipped = 0
    errors = 0
    
    # Create fake search_id for import
    c.execute("INSERT INTO searches (user_id, keywords, source) VALUES (?, ?, ?)", (user_id, keyword, "CSV Import"))
    search_id = c.lastrowid
    
    for line in results:
        try:
            c.execute("INSERT INTO results (search_id, keyword, result_line) VALUES (?, ?, ?)", 
                      (search_id, keyword, line))
            imported += 1
        except sqlite3.IntegrityError:
            skipped += 1
        except Exception:
            errors += 1
            
    conn.commit()
    conn.close()
    
    return {
        "imported": imported,
        "skipped": skipped,
        "errors": errors
    }
