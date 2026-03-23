import os
from dotenv import load_dotenv
import datetime

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
USE_MONGO = bool(MONGODB_URI)

if USE_MONGO:
    from pymongo import MongoClient
    from pymongo.errors import DuplicateKeyError
    
    print("🍃 Using MongoDB backend for persistent storage")
    client = MongoClient(MONGODB_URI)
    
    try:
        db = client.get_default_database()
    except Exception:
        db = client['search_bot']

    
    # Collections structure init
    admins_col = db['admins']
    searches_col = db['searches']
    results_col = db['results']
    
    # Setup indexes for speed and uniqueness
    admins_col.create_index("user_id", unique=True)
    results_col.create_index([("keyword", 1), ("result_line", 1)], unique=True)
    results_col.create_index("keyword")
    
else:
    import sqlite3
    print("💾 Using SQLite backend for storage (Local VPS mode)")
    DB_FILE = os.path.join(os.path.dirname(__file__), "bot_data.db")
    
    def _get_conn():
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db():
        conn = _get_conn()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            keywords TEXT,
            source TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
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


# ==============================================================
# Admin Management
# ==============================================================

def get_admins():
    if USE_MONGO:
        return [doc['user_id'] for doc in admins_col.find()]
    else:
        conn = _get_conn()
        c = conn.cursor()
        c.execute("SELECT user_id FROM admins")
        admins = [row['user_id'] for row in c.fetchall()]
        conn.close()
        return admins

def add_admin(user_id):
    if USE_MONGO:
        try:
            admins_col.insert_one({"user_id": user_id})
            return True
        except DuplicateKeyError:
            return False
    else:
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
    if USE_MONGO:
        admins_col.delete_one({"user_id": user_id})
        return True
    else:
        conn = _get_conn()
        c = conn.cursor()
        c.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
        return True

# ==============================================================
# Search Management
# ==============================================================

def create_search(user_id, search_terms, source):
    keywords = ", ".join(search_terms)
    if USE_MONGO:
        result = searches_col.insert_one({
            "user_id": user_id,
            "keywords": keywords,
            "source": source,
            "timestamp": datetime.datetime.utcnow()
        })
        return str(result.inserted_id)
    else:
        conn = _get_conn()
        c = conn.cursor()
        c.execute("INSERT INTO searches (user_id, keywords, source) VALUES (?, ?, ?)", 
                  (user_id, keywords, source))
        search_id = c.lastrowid
        conn.commit()
        conn.close()
        return search_id

def save_result(search_id, line, term):
    if USE_MONGO:
        try:
            results_col.insert_one({
                "search_id": search_id,
                "keyword": term,
                "result_line": line,
                "timestamp": datetime.datetime.utcnow()
            })
        except DuplicateKeyError:
            pass # Ignore duplicates matching keyword+line
    else:
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
    if USE_MONGO:
        return {
            'total_searches': searches_col.count_documents({}),
            'total_keywords': len(results_col.distinct("keyword")),
            'total_unique_results': results_col.count_documents({}),
            'total_hits': results_col.count_documents({})
        }
    else:
        conn = _get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(id) FROM searches")
        total_searches = c.fetchone()[0]
        
        c.execute("SELECT COUNT(DISTINCT keyword) FROM results")
        total_keywords = c.fetchone()[0]
        
        c.execute("SELECT COUNT(id) FROM results")
        total_unique_results = c.fetchone()[0]
        conn.close()
        
        return {
            'total_searches': total_searches,
            'total_keywords': total_keywords,
            'total_unique_results': total_unique_results,
            'total_hits': total_unique_results
        }

def get_keyword_stats(limit=15):
    if USE_MONGO:
        pipeline = [
            {"$group": {
                "_id": "$keyword",
                "total": {"$sum": 1},
                "last_searched": {"$max": "$timestamp"}
            }},
            {"$sort": {"total": -1}},
            {"$limit": limit}
        ]
        rows = list(results_col.aggregate(pipeline))
        return [{
            'keyword': r['_id'],
            'total_hits': r['total'],
            'unique_hits': r['total'],
            'last_searched': r['last_searched'].strftime("%Y-%m-%d %H:%M:%S") if isinstance(r['last_searched'], datetime.datetime) else r['last_searched']
        } for r in rows]
    else:
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
            'unique_hits': r['total'],
            'last_searched': r['last_searched']
        } for r in rows]

def get_recent_searches(limit=10):
    if USE_MONGO:
        rows = list(searches_col.find().sort("timestamp", -1).limit(limit))
        return [{
            'user_id': r['user_id'],
            'keywords': r['keywords'],
            'source': r['source'],
            'timestamp': r['timestamp'].strftime("%Y-%m-%d %H:%M:%S") if isinstance(r['timestamp'], datetime.datetime) else r['timestamp']
        } for r in rows]
    else:
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
    if USE_MONGO:
        return sorted(results_col.distinct("keyword"))
    else:
        conn = _get_conn()
        c = conn.cursor()
        c.execute("SELECT DISTINCT keyword FROM results ORDER BY keyword ASC")
        rows = c.fetchall()
        conn.close()
        return [r['keyword'] for r in rows]

def get_results_by_keyword(keyword, limit=50000, unique_only=True):
    if USE_MONGO:
        rows = list(results_col.find({"keyword": keyword}).sort("timestamp", -1).limit(limit))
        return [{
            'result_line': r['result_line'],
            'timestamp': r['timestamp'].strftime("%Y-%m-%d %H:%M:%S") if isinstance(r['timestamp'], datetime.datetime) else r['timestamp']
        } for r in rows]
    else:
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
    imported = 0
    skipped = 0
    errors = 0
    
    if USE_MONGO:
        search_result = searches_col.insert_one({
            "user_id": user_id,
            "keywords": keyword,
            "source": "CSV Import",
            "timestamp": datetime.datetime.utcnow()
        })
        search_id = str(search_result.inserted_id)
        
        # Batch inserting is faster, but we need to handle duplicates gracefully
        from pymongo.errors import BulkWriteError
        docs = [{"search_id": search_id, "keyword": keyword, "result_line": line, "timestamp": datetime.datetime.utcnow()} for line in set(results)]
        
        if docs:
            try:
                results_col.insert_many(docs, ordered=False)
                imported = len(docs)
            except BulkWriteError as bwe:
                inserted = bwe.details['nInserted']
                imported = inserted
                skipped = len(docs) - inserted
            except Exception:
                errors = len(docs)
                
    else:
        conn = _get_conn()
        c = conn.cursor()
        
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
