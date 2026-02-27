import sqlite3
import os
from flask import Flask, render_template

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbmonitor.sqlite3")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def dashboard():
    conn = get_db()
    cursor = conn.cursor()

    # --- 1) En güncel skor kaydı ---
    cursor.execute(
        "SELECT id, check_date, score FROM HealthHistory ORDER BY id DESC LIMIT 1"
    )
    latest_row = cursor.fetchone()

    latest = None
    penalties = []
    if latest_row:
        latest = {
            "id": latest_row["id"],
            "check_date": latest_row["check_date"],
            "score": latest_row["score"],
        }

        # --- 2) En güncel kayda ait ceza logları ---
        cursor.execute(
            "SELECT penalty_desc FROM PenaltyLog WHERE history_id = ?",
            (latest_row["id"],),
        )
        penalties = [row["penalty_desc"] for row in cursor.fetchall()]

    # --- 3) Son 20 kontrol (grafik + tablo) ---
    cursor.execute(
        "SELECT id, check_date, score FROM HealthHistory ORDER BY id DESC LIMIT 20"
    )
    history_rows = cursor.fetchall()

    # Her kayda ait cezaları da çek (tablo için)
    history = []
    for row in history_rows:
        cursor.execute(
            "SELECT penalty_desc FROM PenaltyLog WHERE history_id = ?", (row["id"],)
        )
        row_penalties = [p["penalty_desc"] for p in cursor.fetchall()]
        history.append(
            {
                "id": row["id"],
                "check_date": row["check_date"],
                "score": row["score"],
                "penalties": row_penalties,
            }
        )

    conn.close()

    # Grafik için kronolojik sıra (eskiden yeniye)
    chart_labels = [h["check_date"] for h in reversed(history)]
    chart_scores = [h["score"] for h in reversed(history)]

    return render_template(
        "index.html",
        latest=latest,
        penalties=penalties,
        history=history,
        chart_labels=chart_labels,
        chart_scores=chart_scores,
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050)
