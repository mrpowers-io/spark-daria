#!/usr/bin/env bash

function s3_size () {
aws s3 ls "$1" --human-readable --recursive --summarize
}