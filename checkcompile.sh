#!/bin/bash                                                                                                                                                                                                                                    

COMMITS=$(git log --oneline HEAD...182d13c43dec581a84c7edad962dfbd456744a64^ | cut -d " " -f 1)


testCommit() {
    # COMMIT = $1
    echo Checking out commit $COMMIT
    git checkout $COMMIT > /dev/null 2> /dev/null
    
    mvn compile test-compile > /dev/null 2> /dev/null
                                                                                                                                                                                                                                
    if [ $? -eq 0 ]
    then
        echo $COMMIT passed
    else
        echo $COMMIT failed
    fi
}

for COMMIT in $COMMITS
do
    testCommit "$COMMIT"
done


