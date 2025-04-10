#!/bin/bash

myip=$(nmcli device show wlp3s0 | grep IP4.ADDRESS | grep -o -E "[0-9]{3}.*" | cut -f1 -d/)
masterport="50051"
echo -e "myip: $myip\n"

# Kill any existing processes running on the required ports
kill_ports() {
  ports=("50051" "50052" "50053" "50054")
  for port in "${ports[@]}"; do
    fuser -k "${port}/tcp" > /dev/null 2>&1
  done
}

# Start the Master server
start_master() {
  pushd master > /dev/null
  go run main.go &
  MASTER_PID=$!
  popd > /dev/null
}

# Start a DataKeeper instance
start_datakeeper() {
  local id=$1
  local port=$2
  pushd datakeeper > /dev/null
  go run main.go -i="$id" -ip="$myip" -p="$port" -m="$myip:$masterport" &
  eval "DK${id}_PID=$!"
  popd > /dev/null
}

# Main script execution
kill_ports

start_master
sleep 2 # Allow Master server to initialize

start_datakeeper 1 50052

# Wait for user to terminate the script
read -p "Press Enter to stop all servers..."

# Stop all servers
kill $MASTER_PID $DK1_PID 
echo "All servers stopped."