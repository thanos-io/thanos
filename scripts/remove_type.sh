#!/bin/bash

# This script removes a Go type struct definition from a source file,
# modifying the file in-place.
# It keeps the methods associated with the type.
# Usage: ./scripts/remove_type.sh <TypeName> <file.go>

set -eu

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <TypeName> <file.go>" >&2
    exit 1
fi

TYPE_NAME=$1
FILE_PATH=$2

if [ ! -f "$FILE_PATH" ]; then
    echo "Error: File not found at '$FILE_PATH'" >&2
    exit 1
fi

# Create a temporary file to store the modified content.
TMP_FILE=$(mktemp)
# Ensure the temp file is removed on script exit.
trap 'rm -f "$TMP_FILE"' EXIT

# Use awk to parse the file and remove the struct.
# The awk script identifies the 'type <TypeName> struct {' line,
# then counts braces to find the matching closing brace '}'.
# It skips printing all lines within that block.
# All other lines, including method definitions, are printed to the temp file.
awk -v type_to_remove="$TYPE_NAME" '
# Match "type <TypeName> struct {" with flexible whitespace.
BEGIN {
  pattern = "type[[:space:]]+" type_to_remove "[[:space:]]+struct[[:space:]]*{"
}

# When not inside a struct and the pattern matches, start processing the struct.
!in_struct && $0 ~ pattern {
  in_struct = 1
  brace_level = 0
  brace_level += gsub(/{/, "{", $0)
  brace_level -= gsub(/}/, "}", $0)

  if (brace_level <= 0) {
      in_struct = 0
  }
  next
}

# When inside a struct, count braces and skip lines until the end of the struct.
in_struct {
  brace_level += gsub(/{/, "{", $0)
  brace_level -= gsub(/}/, "}", $0)

  if (brace_level <= 0) {
    in_struct = 0
  }
  next
}

# Print all other lines.
{ print }
' "$FILE_PATH" > "$TMP_FILE"

# Replace the original file with the modified content.
mv "$TMP_FILE" "$FILE_PATH"

echo "Struct '$TYPE_NAME' removed from '$FILE_PATH'."
