#!/bin/bash

myip=$(nmcli device show wlp3s0 | grep IP4.ADDRESS | grep -o -E "[0-9]{3}.*" | cut -f1 -d/)
masterip="192.168.1.15"

# Kill any existing processes running on the required ports
kill_ports() {
  ports=("50051" "50052" "50053" "50054")
  for port in "${ports[@]}"; do
    fuser -k "${port}/tcp" > /dev/null 2>&1
  done
}

# Start a DataKeeper instance
start_datakeeper() {
  local id=$1
  local port=$2
  echo "Starting DataKeeper $id on port $port..."
  pushd datakeeper > /dev/null
  go run main.go -i="$id" -ip="$myip" -p="$port" -m="$master:50051" &
  eval "DK${id}_PID=$!"
  popd > /dev/null
}

# Main script execution
kill_ports

start_master
sleep 2 # Allow Master server to initialize

start_datakeeper 2 50053
start_datakeeper 3 50054

echo "DataKeeper 2 PID: $DK2_PID"
echo "DataKeeper 3 PID: $DK3_PID"

# Wait for user to terminate the script
read -p "Press Enter to stop all servers..."

# Stop all servers
kill $MASTER_PID $DK1_PID 
echo "All servers stopped."