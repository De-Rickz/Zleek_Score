# zleek-ntfy.sh: shared ntfy helper for the host scripts. Sourced, not run.
#
# Uses the heartbeat's topic: NTFY_URL / NTFY_TOKEN from the environment, else
# read (only those two keys, never the whole file: it also holds the database
# URL) from ZLEEK_NTFY_ENV, default ~/zleek/apps/worker/.env.heartbeat.
#
#   zleek_ntfy PRIORITY TAGS MESSAGE   -> 0 if delivered (or no topic set), 1 if not

_zleek_env_value() {  # _zleek_env_value KEY FILE: value of KEY=... (quotes and spaces trimmed)
    [ -r "$2" ] || return 0
    sed -n "s/^[[:space:]]*$1[[:space:]]*=[[:space:]]*//p" "$2" | tail -n 1 | sed 's/^["'\'']//; s/["'\'']$//; s/[[:space:]]*$//'
}

zleek_ntfy() {
    local priority="$1" tags="$2" message="$3"
    local env_file="${ZLEEK_NTFY_ENV:-$HOME/zleek/apps/worker/.env.heartbeat}"
    local url="${NTFY_URL:-$(_zleek_env_value NTFY_URL "$env_file")}"
    local token="${NTFY_TOKEN:-$(_zleek_env_value NTFY_TOKEN "$env_file")}"
    if [ -z "$url" ]; then
        echo "zleek_ntfy: no NTFY_URL (env or $env_file); not sent: $message" >&2
        return 0
    fi
    local auth=()
    [ -n "$token" ] && auth=(-H "Authorization: Bearer $token")
    curl -fsS -m 10 --retry 2 -o /dev/null \
         -H "Title: Zleek Score" -H "Priority: $priority" -H "Tags: $tags" \
         ${auth[@]+"${auth[@]}"} --data-raw "$message" "$url"
}
