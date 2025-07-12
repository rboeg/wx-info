#!/usr/bin/env bash

NETWORK_NAME="wx-info-net"
SUBNET="172.20.0.0/16"
GATEWAY="172.20.0.1"

if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "Creating Docker network $NETWORK_NAME..."
  docker network create --driver=bridge --subnet="$SUBNET" --gateway="$GATEWAY" "$NETWORK_NAME"
else
  echo "Docker network $NETWORK_NAME already exists."
fi