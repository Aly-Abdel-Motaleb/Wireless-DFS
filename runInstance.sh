#!/bin/bash

myip="192.168.1.15"

# Kill any existing processes running on the required ports
kill_ports() {
  ports=("50051" "50052" "50053" "50054")
  for port in "${ports[@]}"; do
    fuser -k "${port}/tcp" > /dev/null 2>&1
  done
}

# Start the Master server
start_master() {
  echo "Starting Master server..."
  pushd master > /dev/null
  go run main.go -ip="$myip:50051" &
  MASTER_PID=$!
  popd > /dev/null
}

# Start a DataKeeper instance
start_datakeeper() {
  local id=$1
  local port=$2
  echo "Starting DataKeeper $id on port $port..."
  pushd datakeeper > /dev/null
  go run main.go -i="$id" -ip="$myip" -p="$port" -m="$myip:50051" &
  eval "DK${id}_PID=$!"
  popd > /dev/null
}

# Main script execution
kill_ports

start_master
sleep 2 # Allow Master server to initialize

start_datakeeper 1 50052
start_datakeeper 2 50053
# start_datakeeper 3 50054

echo "All servers are running."
echo "Master PID: $MASTER_PID"
echo "DataKeeper 1 PID: $DK1_PID"
echo "DataKeeper 2 PID: $DK2_PID"
echo "DataKeeper 3 PID: $DK3_PID"

# Wait for user to terminate the script
read -p "Press Enter to stop all servers..."

# Stop all servers
kill $MASTER_PID $DK1_PID $DK2_PID $DK3_PID
echo "All servers stopped."