#!/usr/bin/env bash
# 按 Ctrl+C 退出

# ── 颜色 ──
R='\033[91m'; G='\033[92m'; Y='\033[93m'
B='\033[94m'; M='\033[95m'; C='\033[96m'
W='\033[97m'; DIM='\033[2m'; RST='\033[0m'; BOLD='\033[1m'

COLS=$(tput cols 2>/dev/null || echo 100)

hr() { printf "${DIM}"; printf '─%.0s' $(seq 1 $COLS); printf "${RST}\n"; }

rnd()      { echo $(( RANDOM % ($2 - $1 + 1) + $1 )); }
rnd_ip()   { echo "$(rnd 1 254).$(rnd 1 254).$(rnd 1 254).$(rnd 1 254)"; }
rnd_hex()  { cat /dev/urandom | LC_ALL=C tr -dc 'a-f0-9' | head -c ${1:-16}; }
rnd_port() { local ports=(22 80 443 3306 5432 6379 8080 8443 9200 27017); echo ${ports[RANDOM % ${#ports[@]}]}; }
sleep_r()  { sleep "0.0$(rnd 2 15)"; }   # 20~150ms

MODULES=("core.scheduler" "net.connector" "db.query_optimizer"
         "ml.feature_extractor" "crypto.hasher" "cache.eviction"
         "io.buffer_pool" "auth.token_validator" "rpc.dispatcher"
         "search.index_builder" "log.aggregator" "metric.collector")

ACTIONS=("Initializing" "Compiling" "Linking" "Optimizing"
         "Scanning" "Resolving" "Flushing" "Rebuilding"
         "Syncing" "Verifying" "Encrypting" "Dispatching")

# ── 1. 模块加载进度条 ──
block_modules() {
  hr
  printf "${BOLD}${C}[ MODULE LOADER ]${RST}  ${DIM}pid=$$${RST}\n"
  hr
  local n=${#MODULES[@]}
  for i in $(seq 0 $(( n-1 ))); do
    local mod=${MODULES[$i]}
    local act=${ACTIONS[$(rnd 0 $(( ${#ACTIONS[@]}-1 )))]}
    local width=$(rnd 22 36)
    printf "  ${DIM}%-14s${RST} ${Y}%-32s${RST}  [" "$act" "$mod"
    for _ in $(seq 1 $width); do
      printf "${G}█${RST}"; sleep_r
    done
    printf "]  ${G}✔ OK${RST}\n"
    sleep_r
  done
}

# ── 2. 网络扫描 ──
block_network() {
  hr
  printf "${BOLD}${M}[ NETWORK PROBE ]${RST}\n"
  hr
  local count=$(rnd 8 14)
  for _ in $(seq 1 $count); do
    local ip=$(rnd_ip)
    local port=$(rnd_port)
    local lat_int=$(rnd 1 1200)
    local lat_dec=$(rnd 0 99)
    local lat="${lat_int}.${lat_dec}"
    local status_r=$(rnd 1 8)
    local status="OPEN"; local sc=$G
    [ $status_r -le 2 ] && status="CLOSED" && sc=$DIM
    [ $status_r -eq 3 ] && status="FILTERED" && sc=$Y
    local cc=$G
    [ $lat_int -ge 60 ]  && cc=$Y
    [ $lat_int -ge 200 ] && cc=$R
    printf "  ${C}%-17s${RST}:${Y}%-6s${RST}  latency=${cc}%8sms${RST}  [${sc}%s${RST}]\n" \
      "$ip" "$port" "$lat" "$status"
    sleep "0.0$(rnd 5 18)"
  done
}

# ── 3. 日志流 ──
LEVELS=("INFO " "WARN " "ERROR" "DEBUG" "TRACE")
LEVEL_COLORS=("$G" "$Y" "$R" "$C" "$M")
LOG_MSGS=(
  "Heartbeat received from worker node"
  "Flushing write-ahead log segment"
  "Compacting memtable to L0 SSTable"
  "GC pause detected in mixed collection"
  "JWT token refreshed for active session"
  "Cache hit ratio stabilized"
  "Replica lag within acceptable threshold"
  "Query plan reused from plan cache"
  "Checkpoint written to persistent storage"
  "Connection pool at high utilization"
  "Index build progress checkpoint saved"
  "Snapshot exported to object storage"
  "Rate limiter enforcing per-tenant quota"
  "Dead letter queue depth increasing"
  "Schema migration applied successfully"
  "Bloom filter rebuilt for SSTable"
  "WAL segment archived to cold storage"
  "Leader election completed in cluster"
  "Partition rebalance triggered"
  "Shard migration completed"
)

block_logs() {
  hr
  printf "${BOLD}${Y}[ LIVE LOG STREAM ]${RST}\n"
  hr
  local count=$(rnd 14 22)
  local hh=$(date +%H); local mm=$(date +%M); local ss=$(date +%S)
  for _ in $(seq 1 $count); do
    ss=$(( (ss + $(rnd 0 3)) % 60 ))
    local ms=$(rnd 0 999)
    local li=$(rnd 0 4)
    # weight: INFO heavy
    local rw=$(rnd 1 10)
    [ $rw -le 5 ] && li=0
    [ $rw -eq 6 ] && li=1
    [ $rw -eq 7 ] && li=3
    [ $rw -eq 8 ] && li=4
    [ $rw -ge 9 ] && li=2
    local lv=${LEVELS[$li]}
    local lc=${LEVEL_COLORS[$li]}
    local msg=${LOG_MSGS[$(rnd 0 $(( ${#LOG_MSGS[@]}-1 )))]}
    local src=${MODULES[$(rnd 0 $(( ${#MODULES[@]}-1 )))]}
    printf "  ${DIM}%02d:%02d:%02d.%03d${RST}  ${lc}%s${RST}  ${DIM}%-28s${RST}  ${W}%s${RST}\n" \
      "$hh" "$mm" "$ss" "$ms" "$lv" "$src" "$msg"
    sleep "0.0$(rnd 4 15)"
  done
}

# ── 4. 数据库查询 ──
QUERIES=(
  "SELECT COUNT(*) FROM events WHERE ts > NOW() - INTERVAL '1h'"
  "UPDATE sessions SET last_seen=NOW() WHERE expired=false"
  "INSERT INTO audit_log (uid,action,ts) VALUES (?,?,NOW())"
  "DELETE FROM tmp_cache WHERE created_at < NOW() - INTERVAL '30m'"
  "EXPLAIN ANALYZE SELECT * FROM orders JOIN users USING(uid)"
  "CREATE INDEX CONCURRENTLY idx_ts ON events(ts DESC)"
  "VACUUM ANALYZE metrics"
  "SELECT pg_size_pretty(pg_total_relation_size('events'))"
)

block_db() {
  hr
  printf "${BOLD}${B}[ DATABASE ACTIVITY ]${RST}\n"
  hr
  local count=$(rnd 5 9)
  for _ in $(seq 1 $count); do
    local q=${QUERIES[$(rnd 0 $(( ${#QUERIES[@]}-1 )))]}
    local rows=$(rnd 0 2000000)
    local ms_int=$(rnd 0 4800); local ms_dec=$(rnd 0 9)
    local cc=$G
    [ $ms_int -ge 100 ]  && cc=$Y
    [ $ms_int -ge 1000 ] && cc=$R
    printf "  ${cc}▶${RST}  ${W}%s${RST}\n" "$q"
    printf "     ${DIM}rows=%9s   time=${cc}%5s.%sms${RST}\n\n" "$rows" "$ms_int" "$ms_dec"
    sleep "0.$(rnd 10 35)"
  done
}

# ── 5. 哈希流 ──
ALGOS=("SHA-256" "SHA-512" "BLAKE3 " "MD5    " "HMAC   ")

block_hashes() {
  hr
  printf "${BOLD}${R}[ CRYPTO / HASH STREAM ]${RST}\n"
  hr
  local count=$(rnd 10 16)
  for _ in $(seq 1 $count); do
    local algo=${ALGOS[$(rnd 0 4)]}
    local size=$(rnd 512 1048576)
    local h=$(rnd_hex 64)
    printf "  ${M}%s${RST}  ${DIM}%10s bytes${RST}  ${C}%s${RST}\n" "$algo" "$size" "$h"
    sleep_r
  done
}

# ── 6. 系统指标 ──
bar() {
  local val=$1   # 0-100
  local filled=$(( val / 5 ))
  local empty=$(( 20 - filled ))
  printf "${G}"
  for _ in $(seq 1 $filled); do printf "█"; done
  printf "${DIM}"
  for _ in $(seq 1 $empty);  do printf "░"; done
  printf "${RST}"
}

block_metrics() {
  hr
  printf "${BOLD}${G}[ SYSTEM METRICS ]${RST}\n"
  hr
  local rounds=$(rnd 3 5)
  for _ in $(seq 1 $rounds); do
    local cpu=$(rnd 12 94); local mem=$(rnd 30 88)
    local dsk=$(rnd 5 78);  local net_i=$(rnd 0 980); local net_d=$(rnd 0 9)
    local cc=$G; [ $cpu -ge 50 ] && cc=$Y; [ $cpu -ge 80 ] && cc=$R
    local mc=$G; [ $mem -ge 60 ] && mc=$Y; [ $mem -ge 80 ] && mc=$R
    printf "  CPU  "; bar $cpu; printf "  ${cc}%3d%%${RST}    MEM  "; bar $mem; printf "  ${mc}%3d%%${RST}\n"
    printf "  DISK "; bar $dsk; printf "  ${DIM}%3d%%${RST}    NET  ${C}%3s.%s MB/s${RST}\n\n" "$dsk" "$net_i" "$net_d"
    sleep "0.$(rnd 30 70)"
  done
}

# ── 转场 spinner ──
spinner_pause() {
  local frames=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
  local n=$(rnd 10 22)
  for _ in $(seq 1 $n); do
    local f=${frames[$(rnd 0 9)]}
    printf "\r  ${C}${f}${RST}  ${DIM}processing...${RST}   "
    sleep "0.08"
  done
  printf "\r%${COLS}s\r" ""  # 清行
}

BLOCKS=(block_modules block_network block_logs block_db block_hashes block_metrics)

trap 'printf "\n\n${Y}Interrupted.${RST}\n\n"; exit 0' INT

clear
printf "\n${BOLD}${C}%*s${RST}\n" $(( (COLS + 28) / 2 )) "  SYSTEM ANALYSIS ENGINE  "
printf "${DIM}%*s${RST}\n\n" $(( (COLS + 12) / 2 )) "host: $(hostname)"

while true; do
  # 随机打乱执行顺序
  local_blocks=("${BLOCKS[@]}")
  for i in $(seq $((${#local_blocks[@]} - 1)) -1 1); do
    j=$(rnd 0 $i)
    tmp=${local_blocks[$i]}
    local_blocks[$i]=${local_blocks[$j]}
    local_blocks[$j]=$tmp
  done
  pick=$(rnd 3 ${#local_blocks[@]})
  for i in $(seq 0 $(( pick - 1 ))); do
    ${local_blocks[$i]}
    sleep "0.$(rnd 10 40)"
  done
  spinner_pause
done
