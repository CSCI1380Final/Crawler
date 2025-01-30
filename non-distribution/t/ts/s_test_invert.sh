#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/../../$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}
TEST_URL="https://mytest.url"  

if $DIFF <(cat "$T_FOLDER"/d/myd5.txt | c/invert.sh "$TEST_URL" | sed 's/[[:space:]]//g' | sort) \
         <(cat "$T_FOLDER"/d/myd6.txt | sed 's/[[:space:]]//g' | sort) >&2;
then
    echo "$0 success: inverted index matches"
    exit 0
else
    echo "$0 failure: inverted index mismatch"
    exit 1
fi