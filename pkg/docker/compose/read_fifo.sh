#!/bin/bash

read_fifo_with_timeout() {
  fifo_file_path=${1}
  limit=${2}
  if [[ -z "${fifo_file_path}" || -z "${limit}" ]]; then
    echo "must provide the file to listen to and the timeout"
    exit 1
    fi
  timeout=$(awk "BEGIN{srand(); print srand() + $limit}")
  until [ -s "${fifo_file_path}" ] || [ $(awk 'BEGIN{srand();print srand()}') -gt $timeout ]
  do
    sleep 1
    done

  if ! [[ -s "${fifo_file_path}" ]]; then
    echo "timeout for listening to fifo"
    else
      echo "content in ${fifo_file_path}: $(eval cat ${fifo_file_path})"
    fi
}

read_fifo_with_timeout "$@"
