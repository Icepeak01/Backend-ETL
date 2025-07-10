from flask import Flask, jsonify
from tasks import fetch_all_companies

app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify(status="ok")

@app.route("/fetch-now", methods=["POST"])
def fetch_now():
    # Directly call your ETL function (synchronous)
    result = fetch_all_companies()
    return jsonify(message="Fetched immediately", detail=result)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
