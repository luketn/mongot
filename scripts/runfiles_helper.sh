#!/usr/bin/env bash

# Helper to find runfiles directory
# Usage: 
#   source scripts/runfiles_helper.sh
#   RUNFILES="$(get_runfiles_dir "${BASH_SOURCE[0]}")"

get_runfiles_dir() {
    local script_path="$1"

    if [ -n "${RUNFILES_DIR:-}" ]; then
        echo "${RUNFILES_DIR}"
        return
    fi
    
    if [ -n "${RUNFILES_MANIFEST_FILE:-}" ]; then
        echo "${RUNFILES_MANIFEST_FILE%.runfiles_manifest}.runfiles"
        return
    fi

    # Try common locations relative to script
    if [ -z "$script_path" ]; then
        # Fallback to guessing caller if not provided
        script_path="${BASH_SOURCE[1]}"
    fi
    
    local script_dir="$(dirname "${script_path}")"
    local script_name="$(basename "${script_path}")"
    
    if [ -d "${script_dir}/${script_name}.runfiles" ]; then
        echo "${script_dir}/${script_name}.runfiles"
    elif [ -d "${script_dir}.runfiles" ]; then
        echo "${script_dir}.runfiles"
    else
        # Last resort: try from PWD
        echo "${PWD}/${script_name}.runfiles"
    fi
}

