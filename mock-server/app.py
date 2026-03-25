import json
from flask import Flask, jsonify, request

app = Flask(__name__)

with open("data/customers.json") as f:
    customers = json.load(f)


@app.route("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/customers")
def get_customers():
    page = request.args.get("page", 1, type=int)
    limit = request.args.get("limit", 10, type=int)

    start = (page - 1) * limit
    end = start + limit
    items = customers[start:end]

    return jsonify({
        "data": items,
        "total": len(customers),
        "page": page,
        "limit": limit
    })


@app.route("/api/customers/<customer_id>")
def get_customer(customer_id):
    for c in customers:
        if c["customer_id"] == customer_id:
            return jsonify(c)
    return jsonify({"error": "customer not found"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
