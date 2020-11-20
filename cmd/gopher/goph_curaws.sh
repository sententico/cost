#!/bin/sh

cd aws/cache || exit 1

# TODO: parameterize CUR location & naming pattern
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
    rm $links
fi
exit 0
