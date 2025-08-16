import sqlite3
import json
import datetime
import requests
from flask import Flask, request, jsonify

DB_PATH = 'cache.db'
# Refresh cache if older than this threshold
CACHE_TTL = datetime.timedelta(hours=12)

app = Flask(__name__)

# Initialize database
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS history (
                code TEXT NOT NULL,
                days INTEGER NOT NULL,
                data TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (code, days)
            )
            """
        )

init_db()


def fetch_from_yahoo(code: str, days: int):
    symbol = code.replace('HK.', '') + '.HK'
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={days}d&interval=1d'
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    result = data['chart']['result'][0]
    timestamps = result['timestamp']
    closes = result['indicators']['quote'][0]['close']
    rows = []
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        date = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
        rows.append({'date': date, 'close': close})
    return rows


@app.route('/api/history')
def get_history_kline():
    code = request.args.get('code')
    days = int(request.args.get('days', '365'))
    if not code:
        return jsonify({'error': 'code is required'}), 400

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            'SELECT data, updated_at FROM history WHERE code=? AND days=?',
            (code, days)
        )
        row = cur.fetchone()
        if row:
            data, updated_at = row
            try:
                ts = datetime.datetime.fromisoformat(updated_at)
            except ValueError:
                ts = datetime.datetime.min
            if datetime.datetime.utcnow() - ts < CACHE_TTL:
                return jsonify(json.loads(data))

    # Not in cache; fetch from remote
    rows = fetch_from_yahoo(code, days)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            'REPLACE INTO history (code, days, data, updated_at) VALUES (?, ?, ?, ?)',
            (code, days, json.dumps(rows), datetime.datetime.utcnow().isoformat())
        )
    return jsonify(rows)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
