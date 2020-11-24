#!/bin/bash
set -x

# It may be useful to make a copy of this file to run the bisect against a script outside the repository

# tweak the working tree by merging the hot-fix branch
# and then attempt a build
# alterantively use cherry pick
if      git merge --no-commit --no-ff hot-fix &&
       make
then
       # run project specific test and report its status
       ~/check_test_case.sh
       status=$?
else
       # tell the caller this is untestable
       status=125
fi

# undo the tweak to allow clean flipping to the next commit
git reset --hard

# return control
exit $status

mvn testCompile || exit 125                     # this skips broken builds



# run a maven test
mvn -Dit.test=TransactionAndCommitModeTest#testLowMaxPoll -DskipUTs=true -DfailIfNoTests=false --projects parallel-consumer-core integration-test
