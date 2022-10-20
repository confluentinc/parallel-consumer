#!/bin/bash
#
# Copyright (C) 2020-2022 Confluent, Inc.
#


COMMITS=$(git log --oneline HEAD...parralel-test-fix^ | cut -d " " -f 1)

testCommit() {
  # COMMIT = $1
  echo Checking out commit $COMMIT
  git checkout $COMMIT >/dev/null 2>/dev/null

  #    mvn compile test-compile > /dev/null 2> /dev/null
  mvn license:check

  if [ $? -eq 0 ]; then
    echo $COMMIT passed
  else
    echo $COMMIT failed
  fi
}

for COMMIT in $COMMITS; do
  testCommit "$COMMIT"
done
