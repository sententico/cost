#!/bin/sh

cd cdrs/cache || exit 1

recent="$(date --date='-10 hours' +%Y/%m/%d)"
next="$(date --date='+8 hours' +%Y/%m/%d)"
if [ $recent = $next ]; then
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-us-east-1-127403002470/$recent" &
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-eu-west-1-127403002470/$recent" &
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-eu-west-2-127403002470/$recent" &
else
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-us-east-1-127403002470/$recent" &
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-eu-west-1-127403002470/$recent" &
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-eu-west-2-127403002470/$recent" &

    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-us-east-1-127403002470/$next" &
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-eu-west-1-127403002470/$next" &
    /opt/sententix/bin/s3sync -p 127403002470 -k sinceD=0.5 -lf "mercury-sonus-bucket-prod-eu-west-2-127403002470/$next" &
fi
find . -maxdepth 1 -name "*mercury-sonus-firehose-*" -mmin +1440 -delete
#shopt -s nullglob #set option if available; otherwise, results undesirable but benign
wait

for f in *mercury-sonus-firehose-*~link; do
    echo "#!begin $f"
    zcat $f | grep ^STOP,
    rm $f
    echo "#!end $f"
done
exit 0