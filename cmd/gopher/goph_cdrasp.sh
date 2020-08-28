#!/bin/sh

if [ ! -d cdrs/cache ]; then
    exit 1
fi
cd cdrs/cache
if [ -f .cmon_cdrasp.lock ]; then
    find . -maxdepth 1 -name ".cmon_cdrasp.lock" -mmin +30 -delete
    if [ -f .cmon_cdrasp.lock ]; then
        exit 0
    fi
fi
trap "rm -f .cmon_cdrasp.lock" 0; trap "exit 0" 1 2 3 13 15; touch .cmon_cdrasp.lock

recent="mercury-sonus-bucket-prod-us-east-1-127403002470/$(date --date='-10 hours' +%Y/%m/%d)"
next="mercury-sonus-bucket-prod-us-east-1-127403002470/$(date --date='+8 hours' +%Y/%m/%d)"
if [ $recent = $next ]; then
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.25 -lf $recent &
else
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.25 -lf $recent &
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.25 -lf $next &
fi
find . -maxdepth 1 -name "*mercury-sonus-firehose-*.gz" -mmin +2880 -delete
#shopt -s nullglob #set option if available; otherwise, results undesirable but benign
wait

for f in *mercury-sonus-firehose-*~link; do
    echo "#!begin $f"
    zcat $f | grep ^STOP,
    rm $f
    echo "#!end $f"
done
exit 0