#!/bin/sh

# Note: AWS posts CUR file assemblies to S3, replacing (or adding, as configured) to the prior
# assembly about every 8-10 hours. Assemblies for a prior month are now generally finalized by
# the 2nd of the following month.
#
# Historically, assemblies for the old month continued to receive usage updates through mid-day
# on the 3rd (adding also SP/RI usage, SP/RI unused & taxes; this coincided with delivery of
# preliminary PDF invoices); Premium Support posted by late on the 5th; EDP & CSC discounts
# posted by the 6th or 7th (this or final updates posted from the 12th to as late as the 16th
# coincided with final PDF invoices).

cd aws/cache || exit 1

ACCT=$1                         # default
BUCKET=$2                       # cost-reporting/CUR/hourly
LABEL="20????01-20????01_$3-0*" # hourly

find . -maxdepth 1 -name "`date -d '6 months ago' +%Y%m`01-20*" -delete     # remove 6mo-old assemblies
prior="$(ls -l $LABEL~link)"
umos="`date -d '3 months ago' +%Y%m`|`date -d '2 months ago' +%Y%m`|`date -d 'last month' +%Y%m`|`date +%Y%m`"
${0%/*}/s3sync -p $ACCT -k sinceD=90 -lf $BUCKET "($umos)01-20.*gz" &       # get updated assemblies for current mo & last 3
wait

if [ -n "$prior" ] && [ "$prior" = "$(ls -l $LABEL~link)" ]; then           # ensure updated assemblies are complete
    links="$(ls $LABEL~link)"
    for f in $links; do                                                     # stream updated assemblies to stdout
        echo "#!begin $f"
        zcat $f                 # TODO: handle non-gzip inputs
        echo "#!end $f"
    done
    trap '' 1 2 3 15; rm $links
fi
exit 0
