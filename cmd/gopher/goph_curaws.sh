#!/bin/sh

# Note: AWS posts CUR files, replacing and expanding the prior set about every 10 hours; when
# rolling to a new month, files for the prior month continue to receive usage updates through
# mid-day on the 3rd (adding also RI usage, RI unused & taxes; this coincodes with delivery of
# preliminary PDF invoices); Premium Support posts by late on the 5th; EDP & CSC discounts post
# by the 6th or 7th (this or final updates posted from the 12th to as late as the 16th coincide
# with final PDF invoices).

cd aws/cache || exit 1

# TODO: parameterize CUR location & naming pattern
# TODO: handle subset/singleton file updates to CUR set
CUR="*hourly-[0-9]*"
find . -maxdepth 1 -name "$CUR.csv.gz*" -mmin +86400 -delete
prior="$(ls -l $CUR.csv.gz~link)"
/opt/sententix/bin/s3sync -k sinceD=58 -lf cost-reporting.aspect.com/CUR/xaws-hourly -- "\.csv\.gz$"&
wait

if [ -n "$prior" ] && [ "$prior" = "$(ls -l $CUR.csv.gz~link)" ]; then
    links="$(ls $CUR.csv.gz~link)"
    for f in $links; do
        echo "#!begin $f"
        zcat $f
        echo "#!end $f"
    done
    trap '' 1 2 3 15; rm $links
fi
exit 0
