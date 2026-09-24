#!/usr/bin/env bash
set -Eeuo pipefail

# Limit ordinary outbound traffic while reserving bandwidth for Hetzner
# Storage Boxes. Usage: sudo ./limitPort.sh 800
# Disable:          sudo ./limitPort.sh 0

readonly LINK_MBIT=1000
readonly TC_HANDLE='1:'
readonly STORAGE_CLASS='1:10'
readonly OTHER_CLASS='1:20'
readonly STORAGE_HOSTS=(
  'u654289.your-storagebox.de'
  'u659285.your-storagebox.de'
)

die() {
  echo "limitPort: $*" >&2
  exit 1
}

[[ ${EUID} -eq 0 ]] || die 'run this script as root'
[[ $# -eq 1 ]] || die 'usage: ./limitPort.sh <Mbit/s|0>'
[[ $1 =~ ^[0-9]+$ ]] || die 'limit must be a whole number in Mbit/s'

readonly LIMIT_MBIT=$1
(( LIMIT_MBIT >= 0 && LIMIT_MBIT < LINK_MBIT )) \
  || die "limit must be between 0 and $((LINK_MBIT - 1)) Mbit/s"

for binary in ip tc getent awk sort; do
  command -v "${binary}" >/dev/null 2>&1 || die "required command is missing: ${binary}"
done

readonly INTERFACE="${LIMIT_PORT_INTERFACE:-$(ip -o route show default | awk 'NR == 1 { print $5 }')}"
[[ -n ${INTERFACE} ]] || die 'could not detect the default network interface'
readonly STATE_FILE="/run/firestream-limit-port-${INTERFACE}.state"

disable_limit() {
  if [[ -f ${STATE_FILE} ]] && tc qdisc show dev "${INTERFACE}" | grep -q "qdisc htb ${TC_HANDLE}"; then
    tc qdisc del dev "${INTERFACE}" root
    rm -f -- "${STATE_FILE}"
    echo "limitPort: disabled on ${INTERFACE}; the kernel default queue is restored"
  elif [[ -f ${STATE_FILE} ]]; then
    rm -f -- "${STATE_FILE}"
    echo "limitPort: limiter state was stale; no qdisc was removed"
  else
    echo "limitPort: no limiter installed by this script on ${INTERFACE}"
  fi
}

if (( LIMIT_MBIT == 0 )); then
  disable_limit
  exit 0
fi

storage_ipv4=()
storage_ipv6=()
for host in "${STORAGE_HOSTS[@]}"; do
  while IFS= read -r address; do
    [[ -n ${address} ]] && storage_ipv4+=("${address}")
  done < <(getent ahostsv4 "${host}" | awk '$2 == "STREAM" { print $1 }' | sort -u)
  while IFS= read -r address; do
    [[ -n ${address} ]] && storage_ipv6+=("${address}")
  done < <(getent ahostsv6 "${host}" | awk '$2 == "STREAM" { print $1 }' | sort -u)
done
(( ${#storage_ipv4[@]} + ${#storage_ipv6[@]} > 0 )) \
  || die 'none of the Storage Box hostnames resolved'

readonly STORAGE_RESERVED_MBIT=$((LINK_MBIT - LIMIT_MBIT))
installed=false
rollback() {
  if [[ ${installed} == true ]]; then
    tc qdisc del dev "${INTERFACE}" root 2>/dev/null || true
    rm -f -- "${STATE_FILE}"
  fi
}
trap rollback ERR

# The default class is hard-capped. Storage Box traffic is guaranteed the
# remainder and may borrow unused capacity up to the full physical link.
tc qdisc replace dev "${INTERFACE}" root handle "${TC_HANDLE}" htb default 20 r2q 1000
installed=true
tc class replace dev "${INTERFACE}" parent "${TC_HANDLE}" classid 1:1 \
  htb rate "${LINK_MBIT}mbit" ceil "${LINK_MBIT}mbit"
tc class replace dev "${INTERFACE}" parent 1:1 classid "${STORAGE_CLASS}" \
  htb rate "${STORAGE_RESERVED_MBIT}mbit" ceil "${LINK_MBIT}mbit" prio 0
tc class replace dev "${INTERFACE}" parent 1:1 classid "${OTHER_CLASS}" \
  htb rate "${LIMIT_MBIT}mbit" ceil "${LIMIT_MBIT}mbit" prio 1
tc qdisc replace dev "${INTERFACE}" parent "${STORAGE_CLASS}" handle 10: fq_codel
tc qdisc replace dev "${INTERFACE}" parent "${OTHER_CLASS}" handle 20: fq_codel

filter_priority=10
for address in "${storage_ipv4[@]}"; do
  tc filter add dev "${INTERFACE}" protocol ip parent "${TC_HANDLE}" \
    pref "${filter_priority}" u32 match ip dst "${address}/32" flowid "${STORAGE_CLASS}"
  filter_priority=$((filter_priority + 1))
done
for address in "${storage_ipv6[@]}"; do
  tc filter add dev "${INTERFACE}" protocol ipv6 parent "${TC_HANDLE}" \
    pref "${filter_priority}" u32 match ip6 dst "${address}/128" flowid "${STORAGE_CLASS}"
  filter_priority=$((filter_priority + 1))
done

printf 'interface=%s\nlimit_mbit=%s\nstorage_ipv4=%s\nstorage_ipv6=%s\n' \
  "${INTERFACE}" "${LIMIT_MBIT}" "${storage_ipv4[*]}" "${storage_ipv6[*]}" > "${STATE_FILE}"
installed=false
trap - ERR

echo "limitPort: ${INTERFACE} ordinary outbound traffic capped at ${LIMIT_MBIT} Mbit/s"
echo "limitPort: Storage Boxes (${storage_ipv4[*]} ${storage_ipv6[*]}) may use up to ${LINK_MBIT} Mbit/s"
tc -s class show dev "${INTERFACE}"
