#!/bin/sh

cd tel/cache
if [ -f .cmon_rbbn.lock ]; then
    exit
fi
trap 'rm -f .cmon_rbbn.lock' 0; trap 'exit' 1 2 3 15; touch .cmon_rbbn.lock

s3sync -zlf cost-reporting.aspect.com/CUR/xaws-hourly -- "\.csv\.gz$" 2>/dev/null

zcat *~link | grep -v "^STOP," | csv 
rm *~link

exit 0