#!/bin/bash
# This is a student test


T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/../../$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}

INPUT1="<html><head><title>Test Page</title></head><body><h1>Hello, World!</h1><p>This is a test from zejun.</p></body></html>"
EXPECTED_OUTPUT1=$'HELLO, WORLD!\n\nThis is a test from zejun.'

if $DIFF <(echo "$INPUT1" | c/getText.js | sort) <(echo "$EXPECTED_OUTPUT1" | sort) >&2; then
    echo "$0 success: text are same"
else
    echo "$0 failure: text are not same"
    exit 1  
fi

