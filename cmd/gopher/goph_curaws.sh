#!/bin/sh

# Note: AWS posts CUR file assemblies to S3, replacing (or adding, as configured) to the prior
# assembly about every 10 hours; when rolling to a new month, assemblies for the old month
# continue to receive usage updates through mid-day on the 3rd (adding also RI usage, RI unused &
# taxes; this coincides with delivery of preliminary PDF invoices); Premium Support posts by late
# on the 5th; EDP & CSC discounts post by the 6th or 7th (this or final updates posted from the
# 12th to as late as the 16th coincide with final PDF invoices).

cd aws/cache || exit 1

ACCT=$1                 # default
BUCKET=$2               # cost-reporting/CUR/hourly
LABEL="20????_$3-0*"    # hourly

find . -maxdepth 1 -name "$LABEL" -mmin +133920 -delete
prior="$(ls -l $LABEL~link)"
${0%/*}/s3sync -p $ACCT -k sinceD=90 -lm $BUCKET &
wait

if [ -n "$prior" ] && [ "$prior" = "$(ls -l $LABEL~link)" ]; then
    links="$(ls $LABEL~link)"
    for f in $links; do
        echo "#!begin $f"
        zcat $f         # TODO: handle non-gzip inputs
        echo "#!end $f"
    done
    trap '' 1 2 3 15; rm $links
fi
exit 0
