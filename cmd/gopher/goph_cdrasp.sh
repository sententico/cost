#!/bin/sh

if [ ! -d cdrs/cache ]; then
    exit 1
fi
cd cdrs/cache
if [ -f .cmon_asp.lock ]; then
    exit 0
fi
trap "rm -f .cmon_asp.lock" 0; trap "exit 0" 1 2 3 13 15; touch .cmon_asp.lock

recent="mercury-sonus-bucket-prod-us-east-1-127403002470/$(date --date='-12 hours' +%Y/%m/%d)"
next="mercury-sonus-bucket-prod-us-east-1-127403002470/$(date --date='+6 hours' +%Y/%m/%d)"
if [ $recent == $next ]; then
    s3sync -p 127403002470 -k sinceD=0.125 -lf $recent
else
    s3sync -p 127403002470 -k sinceD=0.125 -lf $recent &
    s3sync -p 127403002470 -k sinceD=0.125 -lf $next &
    wait
fi

for f in *~link; do
    echo "#!begin $f"
    zcat $f | grep ^STOP,
    rm $f
    echo "#!end $f"
done

find . -maxdepth 1 -name "*mercury-sonus-firehose-*.gz" -mmin +2881 -delete

exit 0