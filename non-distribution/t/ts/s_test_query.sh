#!/bin/bash
# This is a student test

T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/../../$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}
QUERY_TERM="apple"  

cp "$T_FOLDER"/d/myd7.txt d/global-index.txt

if $DIFF <(./query.js "$QUERY_TERM") <(cat "$T_FOLDER"/d/myd8.txt) >&2;
then
    echo "$0 success: '$QUERY_TERM' search results matched"
    exit 0
else
    echo "$0 failure: '$QUERY_TERM' search results differ" >&2
    exit 1
fi
