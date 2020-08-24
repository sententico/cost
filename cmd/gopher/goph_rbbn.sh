#!/bin/sh

if [ ! -d cdrs/cache ]; then
    exit 1
fi
cd cdrs/cache
if [ -f .cmon_rbbn.lock ]; then
    exit 0
fi
trap "rm -f .cmon_rbbn.lock" 0; trap "exit 0" 1 2 3 15; touch .cmon_rbbn.lock

# TODO: parameterize profile (with date suffixes) and bucket
b="mercury-sonus-bucket-prod-us-east-1-127403002470/$(date +%Y/%m/%d)"
bn="mercury-sonus-bucket-prod-us-east-1-127403002470/$(date --date='+6 hours' +%Y/%m/%d)"
if [ $b == $bn ]; then
    s3sync -p 127403002470 -k sinceD=0.05 -lf $b
else
    s3sync -p 127403002470 -k sinceD=0.05 -lf $b &
    s3sync -p 127403002470 -k sinceD=0.05 -lf $bn &
    wait
fi

for f in *~link; do
    echo "#!begin $f"
    zcat $f | grep ^STOP,
    rm $f
    echo "#!end $f"
done

find . -maxdepth 1 -name "*gz" -mmin +2881 -delete

exit 0