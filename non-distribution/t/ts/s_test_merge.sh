#!/bin/bash
# This is a student test


T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/../../$R_FOLDER" || exit 1

DIFF=${DIFF:-diff}
DIFF_PERCENT=${DIFF_PERCENT:-0}

rm -f d/global-index.txt 2>/dev/null

test_files=(
    "$T_FOLDER/d/mym1.txt"
    "$T_FOLDER/d/mym2.txt"
    "$T_FOLDER/d/mym3.txt"
)

for input_file in "${test_files[@]}"
do
    echo "Merging: $input_file"
    cat "$input_file" | c/merge.js d/global-index.txt > d/temp-global-index.txt
    mv d/temp-global-index.txt d/global-index.txt
done

if DIFF_PERCENT=$DIFF_PERCENT t/gi-diff.js <(sort d/global-index.txt) <(sort "$T_FOLDER/d/mym4.txt") >&2;
then
    echo "$0 success: merged indexes are identical"
    exit 0
else
    echo "$0 failure: merged indexes differ"
    exit 1
fi