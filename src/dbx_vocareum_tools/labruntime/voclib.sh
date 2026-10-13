#!/usr/bin/env bash
# Shared helpers for the Vocareum Databricks lifecycle scripts.
#
# Sourced, not executed. Vocareum runs only the four named hooks plus grade.sh,
# so this file arrives in /voc/scripts alongside them and `dbx-vocareum-upload`
# warns that nothing executes it. That warning is correct and expected, and it is
# raised for voclab.py too, which the hooks run rather than source.
#
# How each hook finds this file, and why it is not one line. Vocareum guarantees
# neither the working directory a hook runs in nor the form of $0. A real
# session invoked lab_end.sh with a $0 carrying no directory at all, from a
# working directory with no copy of this file, while the setup hooks of the
# same session found it. So neither can be relied on.
#
# So each hook searches: beside $0, then /voc/scripts, then the working
# directory. And each hook carries its own failure branch for when none of those
# holds this file, because voc_fail is defined here, which makes it exactly the
# thing a hook cannot call when this file is what went missing. That branch
# writes error_code MISSING_VOCLIB with plain printf. Without it the hook exits 1
# saying nothing, and the failure can only be explained from the console log.
#
# Everything here exists to make one channel reliable. Vocareum does not expose
# a lifecycle script's standard output through its REST API, so a line written
# with `voc_log` is visible to whoever is watching the Vocareum console and to
# nobody else. The durable channel is voccustomdata.txt: Vocareum forwards it to
# the next script in the sequence as VOC_CUSTOM_DATA, reads error_code and
# message out of it when a script exits non-zero, and surfaces it in the session
# record that `dbx-vocareum-diagnose` reads. So anything a future reader needs
# goes through voc_custom_write, not through echo.

# voccustomdata.txt is written in the working directory, which is where Vocareum
# looks for it. The IPC file is addressed through VOC_IPC_DATA_FILE rather than
# by name, because the variable is the contract and vocipcdata.txt is only what
# the documentation happens to show.
VOC_CUSTOM_DATA_FILE="voccustomdata.txt"

# The Python half of these scripts, uploaded to /voc/scripts beside the hooks and
# never executed by Vocareum. It holds the Databricks calls, because those are
# not calls worth writing in shell. Its header carries the rest of that decision.
VOC_LAB_PYTHON="voclab.py"

# The course's own values, uploaded to /voc/scripts beside the hooks and sourced
# at the bottom of this file. It is what makes these four hooks the same four
# hooks for every course: a cluster runtime, a library list, a catalog name and a
# notebook list belong to a course, and every one of them is a value rather than
# a line of code. `dbx-vocareum-init` scaffolds it from course.env.example.
VOC_COURSE_ENV_FILE="course.env"

# The variables these scripts set for themselves. All begin with VOC, the same
# prefix Vocareum uses, so voc_own_variable excludes them by name. This list is
# deliberately outside that prefix for the same reason. Reporting our own values
# would put them into the one measurement that establishes what Vocareum
# actually hands a hook, which is the measurement the first live run exists for.
LAB_OWN_VOC_VARIABLES=" VOC_CUSTOM_DATA_FILE VOC_SCRIPT_NAME VOC_BEACON_FALLBACK_ORIGIN VOC_LAB_PYTHON VOC_COURSE_ENV_FILE "

voc_own_variable() {
    # Whether a VOC_ variable came from this lab rather than from Vocareum.
    #
    # Two callers, voc_report_env and voc_beacon_variables, and they had the same
    # test written out twice before course.env existed. A course value would have
    # had to be added to both, and the failure mode of adding it to one is a
    # measurement of "what Vocareum hands a hook" quietly listing values the
    # course put there itself.
    #
    # The VOC_COURSE_ prefix is excluded wholesale rather than named, because the
    # set of course values is the course's to extend and this file cannot hold a
    # list of them.
    case "$1" in
        VOC_COURSE_*) return 0 ;;
    esac
    case "$LAB_OWN_VOC_VARIABLES" in
        *" $1 "*) return 0 ;;
    esac
    return 1
}

voc_log() {
    # Stamped so the ordering of two scripts in one session is recoverable.
    printf '[%s] %s: %s\n' \
        "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        "${VOC_SCRIPT_NAME:-lifecycle}" \
        "$*" >&2
}

voc_escape() {
    # Escape a value for inclusion in a JSON string. Backslash first, or it
    # would escape the escapes added after it. Tabs and newlines are the only
    # control characters these scripts can realistically produce.
    printf '%s' "$1" \
        | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' \
        | awk 'BEGIN { ORS="" } { print sep $0; sep="\\n" }'
}

voc_custom_write() {
    # Replace voccustomdata.txt with one JSON object. Takes already-formed
    # "key": value pairs, so a caller that needs a number or a nested object can
    # pass one.
    printf '{%s}\n' "$1" > "$VOC_CUSTOM_DATA_FILE"
    voc_log "wrote $VOC_CUSTOM_DATA_FILE"
}

voc_custom_field() {
    # Emit one JSON string field, escaped. Composed by the caller with commas.
    printf '"%s": "%s"' "$1" "$(voc_escape "$2")"
}

voc_fail() {
    # The only sanctioned way to end a script badly. Vocareum reads error_code
    # and message from voccustomdata.txt and shows them to the student, so a
    # script that exits 1 without writing them produces a lab that failed for no
    # stated reason. error_code is for a human reading a ticket, so it is a
    # short constant rather than a number.
    #
    # ran_at and script go in alongside them because voc_custom_write replaces
    # the whole file, so a failure early in a hook leaves a session record
    # holding nothing but the failure itself, and a record that names no script
    # is indistinguishable from one Vocareum never ran a hook for at all. With
    # these two fields present, a record carrying script proves the hook ran and
    # reached this point, and a record carrying nothing proves it did not. That
    # is the difference between fixing the script and fixing the lab
    # configuration. Setting them here rather than as an early marker in a hook
    # is deliberate: the truncating write is this one, so a marker written
    # before it would be the first thing destroyed. Doing it here also covers
    # all four hooks at once, not just the one that happened to fail.
    local code="$1"
    shift
    voc_log "FAILED $code: $*"
    voc_custom_write "$(
        printf '%s, %s, %s, %s' \
            "$(voc_custom_field ran_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')")" \
            "$(voc_custom_field script "${VOC_SCRIPT_NAME:-lifecycle}")" \
            "$(voc_custom_field error_code "$code")" \
            "$(voc_custom_field message "$*")"
    )"
    # The same failure through the second channel. voccustomdata.txt is the
    # documented one and the one Vocareum shows the student, but it has not been
    # observed reaching the session record, and this has. Written after the
    # custom-data file rather than instead of it: both, because neither is
    # proven on its own, and a beacon overwrites only the beacon that a hook's
    # own entry wrote.
    voc_ipc_beacon "$code"
    exit 1
}

voc_find() {
    # Locate a file uploaded alongside the hooks, printing its path.
    #
    # The same three candidates each hook searches for this file, in the same
    # order, and for the same reason: Vocareum guarantees neither the working
    # directory a hook runs in nor the form of $0. The header of this file
    # records the session that proved it. $0 is the hook's, not this file's,
    # because a shell function does not change $0.
    #
    # A hook cannot use this to find voclib.sh, which is why the search is
    # spelled out inline there as well. It can use it for everything else, and
    # voclab.py is the second thing that needs finding.
    local name="$1" candidate
    for candidate in "$(dirname "$0")/$name" "/voc/scripts/$name" "./$name"; do
        if [ -f "$candidate" ]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    return 1
}

voc_require_python3() {
    # Refuse to continue without the interpreter voclab.py needs.
    #
    # voc_detect_credential reports whether python3 is on the path, but a report
    # is not a guard, and the failure this prevents is otherwise a bare
    # "command not found" on stderr, which the REST API does not expose. Named
    # as its own error code because the fix is a question for Vocareum support
    # rather than a change to any file here.
    if command -v python3 >/dev/null 2>&1; then
        return 0
    fi
    voc_fail MISSING_PYTHON3 \
        "python3 is not on the PATH of the host Vocareum runs this hook on, so the Databricks provisioning in voclab.py cannot run."
}

voc_python() {
    # Run one Python program uploaded beside the hooks and load its result into
    # shell variables. The first argument names the program; the rest are its
    # own.
    #
    # This is the boundary that keeps a Python failure visible. The program logs
    # to stderr, so its narration lands beside voc_log, and returns key=value
    # lines on stdout. Every one of those becomes a <prefix>_<key> variable
    # here, where the prefix is the program's name without .py, so a hook reads
    # voclab_cluster_name from voclab.py and workshop_catalog from a course's
    # own workshop.py. The exit status is returned unchanged.
    #
    # Generalized from voc_voclab, which is now the one-line wrapper below.
    # voclab.py is the runtime's program and a course does not edit it, so a
    # course that provisions anything of its own has a second program to run,
    # and there was no way to run one. Deriving the prefix from the name rather
    # than sharing voclab_ is the whole point: two programs writing one variable
    # namespace means a hook can read a field the other program set, which is a
    # debugging session nobody would enjoy.
    #
    # Both channels stay single-writer. The program writes neither
    # voccustomdata.txt nor $VOC_IPC_DATA_FILE; the calling hook does, from these
    # variables. Two writers on one file is the race this repository avoids
    # everywhere else, and there is no reason to introduce it here.
    #
    # key=value rather than JSON because the shell side then needs no JSON
    # parser, and there is no jq to assume.
    local program="$1"
    shift

    # The prefix, and the error code that names the program when it is missing.
    # tr rather than a bash substitution because `${var^^}` needs bash 4 and
    # this file runs on hosts carrying bash 3.2. The character class is applied
    # rather than assumed, so a program named with a dash still yields a legal
    # variable name instead of failing every key parse silently. voclab.py comes
    # out as `voclab` and MISSING_VOCLAB_PY, which is what it was before.
    local prefix upper
    prefix="$(printf '%s' "${program%.py}" | tr -c 'a-z0-9_' '_')"
    upper="$(printf '%s' "$prefix" | tr 'a-z' 'A-Z')"

    local previous
    previous="$(compgen -v "${prefix}_" 2>/dev/null || true)"
    if [ -n "$previous" ]; then
        # Cleared so that a second call cannot be read through the first call's
        # values. Under set -u an unset variable is an error, which is the
        # behavior wanted: a caller reading a field this run did not produce
        # should fail loudly rather than act on a stale one.
        # shellcheck disable=SC2086
        unset -v $previous
    fi
    # The two fields every caller may read whatever happened, so that a failure
    # with no output still has something to hand voc_fail.
    eval "${prefix}_error_code=''"
    eval "${prefix}_message=''"

    local script
    if ! script="$(voc_find "$program")"; then
        voc_fail "MISSING_${upper}_PY" \
            "$program was not beside the hook, in /voc/scripts, or in the working directory, so the provisioning it holds could not run."
    fi
    voc_require_python3

    # stdout is captured; stderr is inherited, so the program's log reaches the
    # same place voc_log's does. The status is taken explicitly because set -e is
    # suppressed for a function called in a condition, so nothing else here will
    # notice a non-zero exit.
    local output status
    if output="$(python3 "$script" "$@")"; then
        status=0
    else
        status=$?
    fi

    local line key value
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        key="${line%%=*}"
        value="${line#*=}"
        case "$key" in
            "$line" | "" | *[!a-z0-9_]*)
                # No "=" in the line, an empty key, or a key that is not a
                # legal variable name. Logged rather than ignored: unreadable
                # output from this program is itself worth seeing.
                voc_log "unreadable output from $program: $line"
                continue
                ;;
        esac
        # eval rather than `declare -g`, which needs bash 4.2 and is not there on
        # a host carrying bash 3.2. Getting that wrong is worse than it looks:
        # every key parse fails, no prefixed variable is ever set, and the hook
        # dies on the first read with nothing written to voccustomdata.txt, which
        # is the unexplained failure this whole boundary exists to prevent.
        #
        # And eval is safe in this exact form. The key has just been checked
        # against a strict character class, the prefix through the same class
        # above, and \$value is a deferred reference rather than an
        # interpolation, so what eval parses is the fixed text
        # `voclab_cluster_name=$value` and never the value itself. A Databricks
        # error message full of shell metacharacters stays a string.
        eval "${prefix}_$key=\$value"
    done <<< "$output"

    return "$status"
}

voc_voclab() {
    # Run one voclab.py subcommand. The runtime's own program, so it gets a name
    # of its own rather than every hook repeating the filename: voclab.py is
    # what these hooks call for a cluster, a warehouse and a notebook, and which
    # file that is, is the runtime's business and not a course's.
    #
    # Results come back as voclab_<key>, so a caller reads voclab_cluster_name
    # on success and voclab_error_code with voclab_message on failure, exactly as
    # before this became a wrapper.
    voc_python "$VOC_LAB_PYTHON" "$@"
}

voc_ipc_notebook() {
    # Hand the student a landing page. Vocareum reads notebook_url out of the
    # file named by VOC_IPC_DATA_FILE and redirects the learner there, and only
    # lab_setup.sh and user_setup.sh are given that variable.
    local url="$1"
    if [ -z "${VOC_IPC_DATA_FILE:-}" ]; then
        voc_log "VOC_IPC_DATA_FILE is unset, so no notebook URL was handed back"
        return 0
    fi
    printf '{"notebook_url": "%s"}\n' "$(voc_escape "$url")" \
        > "$VOC_IPC_DATA_FILE"
    voc_log "handed back notebook_url=$url"
}

# Where a beacon points when there is no workspace URL to point it at. A real
# page, because the beacon is a URL a student can be redirected to and a dead
# host would turn a diagnostic into a second failure. Only reached when
# VOC_DB_WORKSPACE_URL is unset, which is itself the thing being reported.
VOC_BEACON_FALLBACK_ORIGIN="https://www.databricks.com"

voc_beacon_variables() {
    # The names of the VOC_ variables this hook was handed, space separated.
    # Names only. A value here would go into the session record, which is read
    # and pasted by people diagnosing a lab, and VOC_DB_SP_SECRET is one of the
    # names this can return.
    local names="" name
    for name in $(compgen -v | grep '^VOC' | sort); do
        voc_own_variable "$name" && continue
        names="$names+$name"
    done
    printf '%s' "${names#+}"
}

voc_ipc_beacon() {
    # Prove this hook ran, through the one channel that demonstrably reaches the
    # session record.
    #
    # voc_log writes to stderr, which the REST API does not expose. Whether
    # voccustomdata.txt reaches the record is unverified: one reading found no
    # customdata key anywhere in it. notebook_url is the only script-controlled
    # field measured arriving there, so that is the field a beacon has to use.
    #
    # Called at the top of a hook and again from voc_fail, the record reads
    # three ways instead of two. A landing URL means the hook ran to completion.
    # A beacon means it ran and stopped, and the fragment says where. Neither
    # means Vocareum never invoked it, which points at the lab configuration
    # rather than at anything in this file.
    #
    # The state goes in a fragment because a fragment is never sent to a server,
    # so a beacon that does get followed lands on an ordinary page rather than
    # putting diagnostic text into somebody's access log.
    local state="$1"
    local origin="$VOC_BEACON_FALLBACK_ORIGIN"
    if [ -n "${VOC_DB_WORKSPACE_URL:-}" ]; then
        origin="$(voc_origin "$VOC_DB_WORKSPACE_URL")"
    fi
    voc_ipc_notebook "$(
        printf '%s/#voc-diag=%s&script=%s&at=%s&vars=%s' \
            "$origin" \
            "$state" \
            "${VOC_SCRIPT_NAME:-lifecycle}" \
            "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
            "$(voc_beacon_variables)"
    )"
}

voc_report_env() {
    # Log every VOC_ variable this script was handed. The documented list per
    # hook is what the tools assume; this is how that assumption gets checked
    # against a real run rather than against the documentation. Which is why
    # values are printed at all: a name-only list cannot tell a variable that
    # arrived empty from one that arrived correct.
    #
    # Values, except for the secrets. The pattern is '^VOC', which matches
    # VOCAREUM_ as well as VOC_, so without redaction every hook would print
    # VOC_DB_API_TOKEN, a bearer token, and VOCAREUM_API_KEY, a long lived API
    # key, in full to the console log on every run. Compare voc_beacon_variables and voc_detect_credential,
    # which report by name only for the same reason.
    #
    # Redaction is by name pattern, because the names are what this function
    # already iterates and a new secret handed to a hook will almost certainly
    # carry one of these four words. A character count rather than a fixed mask:
    # a truncated token and a whole one are indistinguishable under 'REDACTED',
    # and the length is the thing a reader needs to tell them apart.
    #
    # case with globs, not ${name,,} or a =~ test, because bash 3.2 has neither
    # and this file has already been bitten by that: see voc_voclab.
    voc_log "environment:"
    local name value
    for name in $(compgen -v | grep '^VOC' | sort); do
        voc_own_variable "$name" && continue
        value="${!name}"
        case "$name" in
            *TOKEN* | *KEY* | *SECRET* | *PASSWORD*)
                if [ -n "$value" ]; then
                    voc_log "  $name=<set, ${#value} chars>"
                else
                    voc_log "  $name=<empty>"
                fi
                ;;
            *)
                voc_log "  $name=$value"
                ;;
        esac
    done
}

voc_origin() {
    # Reduce a workspace URL to scheme and host, dropping any query, fragment,
    # and trailing slash. VOC_DB_WORKSPACE_URL arrives carrying ?o=<workspace id>
    # already, so appending a path to it directly yields a URL with two query
    # strings and a fragment in the wrong place. This is the shell counterpart of
    # normalize_host() in src/dbx_vocareum_tools/databricks.py, and for the same
    # reason: a URL is only comparable and only extensible once it is bare.
    printf '%s' "${1%%[?#]*}" | sed 's:/*$::'
}

voc_workspace_id() {
    # Read the numeric workspace ID out of the o= parameter of a workspace URL,
    # which is the value that identifies the environment in a support thread.
    printf '%s' "${1:-}" \
        | sed -n 's/.*[?&]o=\([0-9][0-9]*\).*/\1/p'
}

voc_detect_credential() {
    # Report what Databricks credential, if any, this script can reach.
    #
    # This is the open question the initial template exists to answer. The
    # documented environment for a Databricks lifecycle script names
    # VOC_DB_WORKSPACE_URL, the user, and the group, and it names no token. So
    # whether a script can call the Databricks API at all, and as whom, is not
    # something to assume: it is something to measure on the first real run and
    # then write down in docs/databricks-labs.md.
    #
    # The four VOC_DB_ names below are not in Vocareum's documented environment
    # for this hook. A working Vocareum-Databricks integration reads them, so
    # they are worth probing even
    # though nothing published says they exist. If they are set, a lifecycle
    # script can call the account API and diagnose the identity planes from
    # inside the lab, which is the capability the whole workshop is waiting on.
    #
    # Probed by name and reported by name. VOC_DB_SP_SECRET is a service
    # principal OAuth secret on an Account admin credential, and this string
    # goes into the session record.
    local found=""
    [ -n "${VOC_DB_API_TOKEN:-}" ] && found="$found VOC_DB_API_TOKEN"
    [ -n "${VOC_DB_ACCOUNT_ID:-}" ] && found="$found VOC_DB_ACCOUNT_ID"
    [ -n "${VOC_DB_SP_APPLICATION_ID:-}" ] && found="$found VOC_DB_SP_APPLICATION_ID"
    [ -n "${VOC_DB_SP_SECRET:-}" ] && found="$found VOC_DB_SP_SECRET"
    [ -n "${DATABRICKS_TOKEN:-}" ] && found="$found DATABRICKS_TOKEN"
    [ -n "${DATABRICKS_HOST:-}" ] && found="$found DATABRICKS_HOST"
    [ -n "${DATABRICKS_CLIENT_ID:-}" ] && found="$found DATABRICKS_CLIENT_ID"
    [ -f "$HOME/.databrickscfg" ] && found="$found ~/.databrickscfg"
    command -v databricks >/dev/null 2>&1 && found="$found databricks-cli"
    command -v curl >/dev/null 2>&1 && found="$found curl"
    command -v python3 >/dev/null 2>&1 && found="$found python3"
    printf '%s' "${found# }"
}

voc_load_course_env() {
    # Put the course's values into the environment, for this hook and for every
    # program it runs.
    #
    # Exported, not just set, which is the whole reason for the `set -a` around
    # the source. voclab.py reads VOC_COURSE_ names out of its own environment,
    # and it is a separate process: a plain shell variable would be visible to
    # the hook and invisible to the program the hook exists to run, which is a
    # course whose settings are silently ignored on the one call that uses them.
    #
    # Missing is not a failure. A course that names nothing gets the defaults in
    # voclab.py, and those defaults are a working single-node cluster with no
    # libraries and no notebooks. Logged rather than silent, because "the course
    # values never loaded" and "the course set these values" are two very
    # different readings of the same cluster.
    local path status
    if ! path="$(voc_find "$VOC_COURSE_ENV_FILE")"; then
        voc_log "no $VOC_COURSE_ENV_FILE found, so course defaults apply"
        return 0
    fi
    set -a
    if . "$path"; then
        status=0
    else
        status=$?
    fi
    set +a
    if [ "$status" -ne 0 ]; then
        voc_fail BAD_COURSE_ENV \
            "$path could not be sourced, so this hook has no course values and would provision something the course did not ask for."
    fi
    voc_log "loaded course values from $path"
}

# Loaded here rather than called by each hook. All four hooks need it, none of
# them has a reason to want it later, and a hook that forgot the call would come
# up with defaults and report a perfectly healthy cluster belonging to no course.
voc_load_course_env
