#!/bin/bash

# Start script for HubSpot Local Webhook Server
# This script starts both the Node.js server and ngrok with your static domain

echo "🚀 Starting HubSpot Local Webhook Server..."
echo ""

# Start Node.js server in the background
echo "📡 Starting Node.js server on port 3000..."
node index.js &
NODE_PID=$!
echo "   ✓ Node.js server started (PID: $NODE_PID)"
echo ""

# Wait a moment for the server to start
sleep 2

# Start ngrok with static domain
echo "🌐 Starting ngrok with static domain..."
ngrok http 3000 --domain=uniteable-nongranular-dudley.ngrok-free.dev &
NGROK_PID=$!
echo "   ✓ Ngrok tunnel started (PID: $NGROK_PID)"
echo ""

echo "================================================================"
echo "✅ Services Running!"
echo "================================================================"
echo "📍 Local Server:  http://localhost:3000"
echo "🌍 Public URL:    https://uniteable-nongranular-dudley.ngrok-free.dev"
echo "🔗 Webhook URL:   https://uniteable-nongranular-dudley.ngrok-free.dev/webhook"
echo "📊 Ngrok Dashboard: http://localhost:4040"
echo ""
echo "Press Ctrl+C to stop all services"
echo "================================================================"
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping services..."
    kill $NODE_PID 2>/dev/null
    kill $NGROK_PID 2>/dev/null
    echo "✓ All services stopped"
    exit 0
}

# Trap Ctrl+C and call cleanup
trap cleanup SIGINT SIGTERM

# Keep script running
wait
