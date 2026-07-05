#!/usr/bin/env bash

CUDA_MAJOR=$(nvidia-smi 2>/dev/null | grep -iE 'CUDA.*Version' | head -n1 \
             | awk -F 'CUDA' '{print $NF}' | tr -cd '0-9.' | cut -d. -f1)

if [ "${CUDA_MAJOR:-0}" -ge 13 ]; then
    MINER_BIN="./btx-miner-cu13"
else
    MINER_BIN="./btx-miner-cu12"
fi

echo "[pick-miner] CUDA major=${CUDA_MAJOR:-none} -> $MINER_BIN" >&2
printf '%s\n' "$MINER_BIN"
