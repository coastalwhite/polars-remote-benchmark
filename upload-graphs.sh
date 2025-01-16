#!/bin/sh

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "Did not supply AWS keys..." > /dev/stderr
    exit 2
fi

if [ -z "$AWS_URI" ]; then
    echo "Did not supply AWS URI..." > /dev/stderr
    exit 2
fi

OUT_DIR=./output
aws s3 sync "$OUT_DIR/" "$AWS_URI"