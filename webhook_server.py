from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Dict, Any, List
import uvicorn
from datetime import datetime
import json

app = FastAPI(title="TradingView Webhook Server")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store signals in memory (you can use database later)
signals_history: List[Dict[str, Any]] = []

class WebhookData(BaseModel):
    symbol: str
    exchange: str
    signal: str
    entry_price: float
    stop_loss: float
    take_profit: float
    rsi: float
    ema20: float
    ema50: float
    confidence: int
    timestamp: float
    strategy: str
    sensitivity: str

# =============================================================================
# WEBHOOK ENDPOINT (Receives from TradingView)
# =============================================================================

@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        # Parse the incoming JSON
        body = await request.body()
        data = json.loads(body)
        
        print(f"📥 Received webhook: {data}")
        
        # Store signal
        signal_data = {
            **data,
            "received_at": datetime.now().isoformat(),
            "id": len(signals_history) + 1
        }
        signals_history.append(signal_data)
        
        # Keep only last 100 signals
        if len(signals_history) > 100:
            signals_history.pop(0)
        
        # Broadcast to all connected Flutter clients (using WebSocket or SSE)
        # For now, just return the signal
        
        return {
            "status": "success",
            "message": "Signal received",
            "signal": data.get("signal"),
            "symbol": data.get("symbol")
        }
        
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# GET SIGNALS FOR FLUTTER
# =============================================================================

@app.get("/signals")
async def get_signals(symbol: str = None, limit: int = 50):
    """Get recent signals for Flutter app"""
    if symbol:
        filtered = [s for s in signals_history if s.get("symbol") == symbol]
        return {"signals": filtered[-limit:]}
    return {"signals": signals_history[-limit:]}

@app.get("/latest_signal/{symbol}")
async def get_latest_signal(symbol: str):
    """Get latest signal for a specific symbol"""
    symbol_signals = [s for s in signals_history if s.get("symbol") == symbol]
    if symbol_signals:
        return {"signal": symbol_signals[-1]}
    return {"signal": None}

# =============================================================================
# REAL-TIME UPDATES (WebSocket or SSE)
# =============================================================================

# Option 1: Using WebSocket (Real-time)
from fastapi import WebSocket, WebSocketDisconnect

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# =============================================================================
# DASHBOARD
# =============================================================================

@app.get("/")
async def root():
    return {
        "message": "TradingView Webhook Server",
        "endpoints": {
            "webhook": "POST /webhook (from TradingView)",
            "signals": "GET /signals (for Flutter)",
            "latest": "GET /latest_signal/{symbol}",
            "websocket": "ws://localhost:5000/ws (real-time)"
        },
        "total_signals": len(signals_history)
    }

@app.get("/dashboard")
async def dashboard():
    """HTML dashboard to see signals"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>TradingView Signals Dashboard</title>
        <style>
            body { font-family: Arial; background: #1a1a1a; color: white; padding: 20px; }
            .signal { padding: 10px; margin: 10px 0; border-radius: 8px; }
            .BUY { background: #2d5a2d; border-left: 5px solid #4CAF50; }
            .SELL { background: #5a2d2d; border-left: 5px solid #f44336; }
            .BREAKEVEN { background: #5a5a2d; border-left: 5px solid #FFC107; }
            .EXIT { background: #3a3a3a; border-left: 5px solid #9e9e9e; }
            .symbol { font-size: 1.5em; font-weight: bold; }
            .price { font-size: 1.2em; }
            .timestamp { font-size: 0.8em; color: #aaa; }
            .confidence { font-size: 0.9em; color: #aaa; }
            .strategy { font-size: 0.8em; color: #aaa; }
        </style>
    </head>
    <body>
        <h1>📈 TradingView Webhook Signals</h1>
        <p>Total signals: """ + str(len(signals_history)) + """</p>
        <div id="signals"></div>
        <script>
            async function loadSignals() {
                const response = await fetch('/signals');
                const data = await response.json();
                const container = document.getElementById('signals');
                container.innerHTML = '';
                
                data.signals.reverse().forEach(signal => {
                    const div = document.createElement('div');
                    div.className = 'signal ' + signal.signal;
                    div.innerHTML = `
                        <div class="symbol">${signal.symbol} - ${signal.signal}</div>
                        <div class="price">Entry: ₹${signal.entry_price?.toFixed(2)} | SL: ₹${signal.stop_loss?.toFixed(2)} | TP: ₹${signal.take_profit?.toFixed(2)}</div>
                        <div class="confidence">RSI: ${signal.rsi?.toFixed(1)} | EMA20: ${signal.ema20?.toFixed(2)} | EMA50: ${signal.ema50?.toFixed(2)}</div>
                        <div class="strategy">${signal.strategy} | Sensitivity: ${signal.sensitivity}</div>
                        <div class="timestamp">${new Date(signal.timestamp * 1000).toLocaleString()}</div>
                    `;
                    container.appendChild(div);
                });
            }
            
            loadSignals();
            setInterval(loadSignals, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

# =============================================================================
# RUN SERVER
# =============================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)