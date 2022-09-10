#!/bin/sh

cd cdrs/cache || exit 1

ACCT="127403002470" # TODO: parameterize, assigning from $1 (removing credentials from script)
CDR="*mercury-sonus-firehose-*"
recent="$(date --date='-14 hours' +%Y/%m/%d)"
next="$(date --date='+8 hours' +%Y/%m/%d)"
if [ $recent = $next ]; then
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-us-east-1-$ACCT/$recent" &
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-eu-west-1-$ACCT/$recent" &
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-eu-west-2-$ACCT/$recent" &
else
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-us-east-1-$ACCT/$recent" &
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-eu-west-1-$ACCT/$recent" &
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-eu-west-2-$ACCT/$recent" &

    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-us-east-1-$ACCT/$next" &
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-eu-west-1-$ACCT/$next" &
    ${0%/*}/s3sync -p $ACCT -k sinceD=0.6 -lf "mercury-sonus-bucket-prod-eu-west-2-$ACCT/$next" &
fi
find . -maxdepth 1 -name "$CDR" -mmin +2640 -delete
wait

for f in $(ls $CDR~link); do
    echo "#!begin $f"
    zcat $f | grep ^STOP,
    rm $f
    echo "#!end $f"
done
exit 0
