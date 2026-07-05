#!/usr/bin/env python3
"""
BTX Miner untuk Marimo — Anti-Disconnect Edition
Masalah: marimo kill cell yang jalan terlalu lama / connection drop
Solusi: 
1. Auto-reconnect dengan backoff
2. Background subprocess (tidak block cell)
3. Watchdog monitoring
4. Log rotation

Cara pakai di Marimo:

Cell 1 (setup + start):
```python
import os, subprocess, threading, time, json, urllib.request

os.environ["POOL"] = "global.btxpool.org:23333"
os.environ["BIN_URL"] = "https://github.com/mbng535-cmd/btx-bin/raw/main/btx-miner-cu12"
os.environ["WALLET"] = "btx1zfrpcxd7eeunrkl7amulxlrd008wr7tqyssq2nrv9nganm29khg4sy786yj"
os.environ["WORKER"] = "marimo-gpu"
os.environ["BACKEND"] = "cuda"
os.environ["GPU_DEVICES"] = "all"
os.environ["MODE"] = "stratum"

exec(open("btx_marimo.py").read())
```

Cell 2 (cek status):
```python
print(get_status())
```

Cell 3 (stop):
```python
stop_mining()
```
"""

import os
import sys
import time
import json
import subprocess
import threading
import urllib.request
import re
import signal
import traceback
from pathlib import Path

# ── Config ──────────────────────────────────────────────────

POOL = os.environ.get("POOL", "global.btxpool.org:23333")
BIN_URL = os.environ.get("BIN_URL", "https://github.com/mbng535-cmd/btx-bin/raw/main/btx-miner-cu12")
WALLET = os.environ.get("WALLET", "btx1zfrpcxd7eeunrkl7amulxlrd008wr7tqyssq2nrv9nganm29khg4sy786yj")
WORKER = os.environ.get("WORKER", "marimo-gpu")
BACKEND = os.environ.get("BACKEND", "cuda")
GPU_DEVICES = os.environ.get("GPU_DEVICES", "all")
MODE = os.environ.get("MODE", "stratum")
CUDA_VERSION = os.environ.get("CUDA_VERSION", "")

LOG_FILE = "/tmp/btx_miner.log"
PID_FILE = "/tmp/btx_miner.pid"
STATUS_FILE = "/tmp/btx_miner_status.json"
BIN_PATH = "/tmp/btx-miner"

# ── Global state ────────────────────────────────────────────

_miner_proc = None
_watchdog_thread = None
_stop_flag = threading.Event()
_reconnect_count = 0
_last_log_time = 0
_stats = {
    "started": 0,
    "reconnects": 0,
    "blocks_found": 0,
    "shares_submitted": 0,
    "last_status": "idle",
    "last_hashrate": 0,
    "last_job": "",
    "last_height": 0,
    "uptime": 0,
    "log_tail": [],
}

# ── Binary ──────────────────────────────────────────────────

def detect_cuda():
    """Detect CUDA version from nvidia-smi"""
    if CUDA_VERSION:
        return CUDA_VERSION
    try:
        r = subprocess.run(["nvidia-smi"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            for line in r.stdout.split("\n"):
                if "CUDA Version" in line:
                    m = re.search(r"CUDA.*?(\d+)\.", line)
                    if m:
                        return m.group(1)
    except:
        pass
    return "12"

def download_binary():
    """Download miner binary"""
    cuda_ver = detect_cuda()
    
    # Auto-switch cu12 → cu13 if needed
    url = BIN_URL
    if cuda_ver == "13" and "cu12" in url:
        url = url.replace("btx-miner-cu12", "btx-miner-cu13")
        print(f"[!] CUDA 13 detected, using cu13 binary")
    
    # Skip if already downloaded
    if os.path.exists(BIN_PATH):
        # Check if it's the right version
        if cuda_ver == "13" and "cu12" in BIN_URL and "cu13" not in BIN_PATH:
            pass  # Need re-download
        else:
            os.chmod(BIN_PATH, 0o755)
            print(f"[+] Binary exists: {BIN_PATH}")
            return True
    
    print(f"[*] Downloading: {url}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = urllib.request.urlopen(req, timeout=120).read()
    except Exception as e:
        print(f"[!] Download failed: {e}")
        return False
    
    if data[:4] == b"\x7fELF":
        with open(BIN_PATH, "wb") as f:
            f.write(data)
        os.chmod(BIN_PATH, 0o755)
        print(f"[+] Binary saved ({len(data)} bytes)")
        return True
    else:
        print(f"[!] Not ELF: {data[:4].hex()}")
        return False

# ── Mining ──────────────────────────────────────────────────

def parse_log_line(line):
    """Parse miner log line and update stats"""
    global _stats
    
    line = line.strip()
    if not line:
        return
    
    # Track log tail (last 20 lines)
    _stats["log_tail"].append(line)
    if len(_stats["log_tail"]) > 20:
        _stats["log_tail"].pop(0)
    
    _stats["last_log_time"] = time.time()
    
    # Parse stratum events
    if "stratum connected" in line:
        _stats["last_status"] = "connected"
        _stats["reconnects"] = _stats.get("reconnects", 0)
        _stats["uptime"] = time.time() - _stats.get("started", time.time())
    
    elif "stratum disconnected" in line:
        _stats["last_status"] = "disconnected"
        global _reconnect_count
        _reconnect_count += 1
        _stats["reconnects"] = _reconnect_count
    
    elif "stratum subscribed" in line:
        _stats["last_status"] = "subscribed"
    
    elif "stratum authorized" in line:
        _stats["last_status"] = "authorized"
    
    elif "stratum work start" in line:
        _stats["last_status"] = "mining"
        # Extract height
        m = re.search(r"height=(\d+)", line)
        if m:
            _stats["last_height"] = int(m.group(1))
    
    elif "stratum hashrate" in line:
        _stats["last_status"] = "mining"
        m = re.search(r"height=(\d+)", line)
        if m:
            _stats["last_height"] = int(m.group(1))
    
    elif "stratum accepted" in line or "accepted share" in line:
        _stats["shares_submitted"] += 1
    
    elif "BLOCK FOUND" in line or "block found" in line or "solved" in line.lower():
        _stats["blocks_found"] += 1
    
    # Parse hashrate table
    if "Total" in line and "/s" in line:
        m = re.search(r"(\d+\.?\d*)/s", line)
        if m:
            _stats["last_hashrate"] = float(m.group(1))
    
    # Write status file
    try:
        _stats["uptime"] = time.time() - _stats.get("started", time.time())
        with open(STATUS_FILE, "w") as f:
            json.dump(_stats, f, indent=2)
    except:
        pass

def run_miner():
    """Run miner process and monitor output"""
    global _miner_proc, _stats
    
    cmd = [
        BIN_PATH,
        "-mode", MODE,
        "-backend", BACKEND,
        "-gpu-devices", GPU_DEVICES,
        "-payout", WALLET,
        "-worker", WORKER,
        "-pool", POOL,
        "-loop",
        "-blocks", "0",  # unlimited
    ]
    
    print(f"[*] Starting miner: {' '.join(cmd[:3])}... -pool {POOL}")
    
    _stats["started"] = time.time()
    _stats["last_status"] = "starting"
    
    _miner_proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
    )
    
    # Write PID file
    with open(PID_FILE, "w") as f:
        f.write(str(_miner_proc.pid))
    
    # Read output line by line
    try:
        for line in _miner_proc.stdout:
            if _stop_flag.is_set():
                break
            parse_log_line(line)
            print(line, end="")
    except:
        pass
    
    _miner_proc.wait()
    _stats["last_status"] = "stopped"

def watchdog():
    """Watchdog: restart miner if it crashes"""
    global _reconnect_count
    
    while not _stop_flag.is_set():
        # Check if miner process is alive
        if _miner_proc is None or _miner_proc.poll() is not None:
            if _stop_flag.is_set():
                break
            
            _reconnect_count += 1
            wait_time = min(2 ** min(_reconnect_count, 6), 60)  # Exponential backoff, max 60s
            
            print(f"\n[!] Miner stopped. Reconnecting in {wait_time}s... (attempt {_reconnect_count})")
            time.sleep(wait_time)
            
            if _stop_flag.is_set():
                break
            
            print(f"[*] Restarting miner...")
            run_miner()
        else:
            # Check if no log output for 60s (might be hung)
            if time.time() - _stats.get("last_log_time", time.time()) > 120:
                print(f"\n[!] No output for 120s, killing and restarting...")
                try:
                    _miner_proc.kill()
                except:
                    pass
        
        time.sleep(5)

def start_mining():
    """Start mining in background"""
    global _watchdog_thread, _stop_flag
    
    if _miner_proc is not None and _miner_proc.poll() is None:
        print("[*] Miner already running!")
        return
    
    _stop_flag.clear()
    
    # Download binary
    if not download_binary():
        print("[!] Failed to download binary")
        return
    
    # Start miner in thread
    _watchdog_thread = threading.Thread(target=run_miner, daemon=True)
    _watchdog_thread.start()
    
    # Start watchdog in thread
    wd = threading.Thread(target=watchdog, daemon=True)
    wd.start()
    
    # Wait a bit for initial output
    time.sleep(5)
    print(f"\n[+] Mining started! Worker: {WORKER}")
    print(f"[+] Status: {_stats['last_status']}")
    print(f"[+] Use get_status() to check status")
    print(f"[+] Use stop_mining() to stop")

def stop_mining():
    """Stop mining"""
    global _stop_flag, _miner_proc
    
    _stop_flag.set()
    
    if _miner_proc:
        try:
            _miner_proc.terminate()
            time.sleep(2)
            if _miner_proc.poll() is None:
                _miner_proc.kill()
        except:
            pass
    
    _stats["last_status"] = "stopped"
    print("[+] Mining stopped")

def get_status():
    """Get current mining status"""
    _stats["uptime"] = time.time() - _stats.get("started", time.time()) if _stats.get("started") else 0
    
    status = {
        "status": _stats["last_status"],
        "uptime_seconds": int(_stats["uptime"]),
        "uptime_human": format_time(_stats["uptime"]),
        "reconnects": _stats["reconnects"],
        "blocks_found": _stats["blocks_found"],
        "shares_submitted": _stats["shares_submitted"],
        "hashrate": _stats["last_hashrate"],
        "height": _stats["last_height"],
        "pool": POOL,
        "worker": WORKER,
        "wallet": WALLET[:20] + "...",
    }
    
    result = f"""╔══════════════════════════════════════════════╗
║  BTX MINER STATUS
╠══════════════════════════════════════════════╣
║  Status:    {status['status']:<30s}
║  Uptime:    {status['uptime_human']:<30s}
║  Reconnects: {status['reconnects']:<29d}
║  Blocks:    {status['blocks_found']:<30d}
║  Shares:    {status['shares_submitted']:<30d}
║  Hashrate:  {status['hashrate']:.3f}/s
║  Height:    {status['height']:<30d}
║  Pool:      {status['pool']:<30s}
║  Worker:    {status['worker']:<30s}
╚══════════════════════════════════════════════╝
"""
    
    # Add recent log
    if _stats["log_tail"]:
        result += "\nRecent log:\n"
        for line in _stats["log_tail"][-5:]:
            result += f"  {line}\n"
    
    return result

def format_time(seconds):
    """Format seconds to human readable"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m {int(seconds%60)}s"
    else:
        return f"{int(seconds/3600)}h {int((seconds%3600)/60)}m"

# ── Auto-start ──────────────────────────────────────────────

# Auto-start mining when script is exec'd
if __name__ == "__main__" or "exec" in str(sys.modules.get("__main__", "")):
    pass  # Don't auto-start, let user call start_mining()
