from flask import Flask, request, jsonify
from datetime import datetime
import json
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Store signals in memory (use database in production)
signals_store = []


@app.route("/webhook", methods=["POST"])
def tradingview_webhook():
    """
    Endpoint to receive TradingView webhook signals
    Expected JSON format:
    {
        "symbol": "BTCUSDT",
        "timeframe": "1h",
        "signal": "buy" or "sell",
        "price": 50000.00,
        "signal_strength": 1-4,
        "indicators": {
            "smart_trail_direction": "long/short",
            "trend_catcher_color": "#02ff65",
            "trend_strength": 45.5,
            "volatility": "Moderate",
            "squeeze_metric": 75.0,
            "volume_sentiment": 65.0
        },
        "timestamp": "2024-01-01T12:00:00Z"
    }
    """
    try:
        # Get JSON data from request
        data = request.get_json()

        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Validate required fields
        required_fields = ["symbol", "signal", "price"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Add server timestamp
        data["received_at"] = datetime.utcnow().isoformat()

        # Process the signal
        processed_signal = process_signal(data)

        # Store signal
        signals_store.append(processed_signal)

        # Keep only last 1000 signals
        if len(signals_store) > 1000:
            signals_store.pop(0)

        logger.info(
            f"Signal received: {data['symbol']} - {data['signal']} @ {data['price']}"
        )

        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Signal received",
                    "signal_id": processed_signal["id"],
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500


def process_signal(data):
    """Process and enrich the signal data"""
    signal = {
        "id": f"{data['symbol']}_{datetime.utcnow().timestamp()}",
        "symbol": data["symbol"],
        "timeframe": data.get("timeframe", "1h"),
        "signal": data["signal"],
        "price": data["price"],
        "signal_strength": data.get("signal_strength", 1),
        "timestamp": data.get("timestamp", datetime.utcnow().isoformat()),
        "received_at": data["received_at"],
    }

    # Add indicator data if present
    if "indicators" in data:
        signal["indicators"] = data["indicators"]

    # Add take profit and stop loss if present
    if "tp_levels" in data:
        signal["tp_levels"] = data["tp_levels"]

    if "sl_levels" in data:
        signal["sl_levels"] = data["sl_levels"]

    return signal


@app.route("/signals", methods=["GET"])
def get_signals():
    """Get all stored signals"""
    symbol = request.args.get("symbol")
    limit = request.args.get("limit", 100, type=int)

    filtered_signals = signals_store

    if symbol:
        filtered_signals = [s for s in signals_store if s["symbol"] == symbol]

    return (
        jsonify(
            {
                "count": len(filtered_signals[-limit:]),
                "signals": filtered_signals[-limit:],
            }
        ),
        200,
    )


@app.route("/signals/<signal_id>", methods=["GET"])
def get_signal(signal_id):
    """Get a specific signal by ID"""
    signal = next((s for s in signals_store if s["id"] == signal_id), None)

    if signal:
        return jsonify(signal), 200
    else:
        return jsonify({"error": "Signal not found"}), 404


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return (
        jsonify(
            {
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "signals_count": len(signals_store),
            }
        ),
        200,
    )


@app.route("/stats", methods=["GET"])
def get_stats():
    """Get statistics about signals"""
    if not signals_store:
        return jsonify({"message": "No signals available"}), 200

    buy_signals = sum(1 for s in signals_store if s["signal"] == "buy")
    sell_signals = sum(1 for s in signals_store if s["signal"] == "sell")

    symbols = list(set(s["symbol"] for s in signals_store))

    return (
        jsonify(
            {
                "total_signals": len(signals_store),
                "buy_signals": buy_signals,
                "sell_signals": sell_signals,
                "symbols": symbols,
                "latest_signal": signals_store[-1] if signals_store else None,
            }
        ),
        200,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
