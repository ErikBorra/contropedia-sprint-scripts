#!/bin/bash

# define locations of input files
thread_metrics_tree_string='data/top20_thread_metrics_tree_string.csv'
#discussions_compact_text='data/top20_discussions_compact_text.csv'
discussions_compact_text='../../WikiTalkParser/discussions/discussions_text.csv'
thread_titles='data/top20_thread_titles.csv'

# LOAD MYSQL CONFIG
source db.inc

# TAKE PAGE ARG AS INPUT
page=$(echo $1 | sed 's/ /_/g')
datadir="data/$page"

# Function to get a cache id for each query
function escapeit {
  perl -e 'use URI::Escape; print uri_escape shift();print"\n"' $1 |
   sed 's/\s/_/g' |
   md5sum | sed 's/\s.*$//';
}
# Function to download queries with a cache
mkdir -p "$datadir/.cache"
function download {
  cache="$datadir/.cache/$(escapeit $1)"
  if [ ! -s "$cache" ]; then
    echo "DOWNLOAD $1" >&2
    touch "$cache"
    ct=0
    while [ $ct -lt 3 ]; do
      curl -f -s -L "$1" > "$cache.tmp"
      if [ -s "$cache.tmp" ]; then
        mv "$cache.tmp" "$cache"
        break
      fi
    done
  fi
  cat "$cache"
}

# make revisions.tsv
`sudo rm /tmp/revisions.tsv`
`echo "SELECT r.id, r.user, r.timestamp, r.hash, r.comment FROM revisions r, article a, article_revisions ar WHERE r.id = ar.revision_id AND ar.article_id = a.id AND a.title = '${page}' INTO OUTFILE '/tmp/revisions.tsv' FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'" | mysql -u $MYSQLUSER -p$MYSQLPASS $MYSQLDB`
echo "rev_id\trev_user\trev_timestamp\trev_hash\trev_comment" > "$datadir/revisions.tsv"
`cat /tmp/revisions.tsv >> $datadir/revisions.tsv`

revisions_ids=`sed '1d' $datadir/revisions.tsv | cut -f1`
revisions_list=$(echo "$revisions_ids" | tr '\n' ',')

# Download list of sections in each revision of the page from the API
if [ ! -s "$datadir/sections.tsv" ]; then
  rm -f "$datadir/sections.tmp"
  for revid in $revisions_ids; do
    download "https://en.wikipedia.org/w/api.php?action=parse&oldid=$revid&prop=sections|revid&format=json"   |
    	sed -e $'s/","number/\\\n/g' | grep -v ']}}' | sed 's/^.*"line":"//' | sed 's/^\(.*\)$/\1/' >> "$datadir/sections.tmp"
  done
  sort -u "$datadir/sections.tmp" > "$datadir/sections.tsv"
  rm "$datadir/sections.tmp"
fi

# Get the association of revisions with sections from Erik's database
if [ ! -s "$datadir/revisions_sections.tsv" ]; then
	select=$(echo "SELECT to_revision_id as revision_id, raw_element as section_name FROM element_edit WHERE to_revision_id IN ($revisions_list) GROUP BY to_revision_id, raw_element" | sed 's/,)/)/g')
  echo "$select" | mysql -u $MYSQLUSER -p$MYSQLPASS $MYSQLDB > "$datadir/revisions_sections.tsv"
fi

# Extract discussions from David's data
head -n 1 $discussions_compact_text | iconv -f "iso8859-1" -t "UTF-8" > "$datadir/discussions.tsv"
grep -P "^([^\t]+\t){5}$pageid\t" $discussions_compact_text | iconv -f "iso8859-1" -t "UTF-8" >> "$datadir/discussions.tsv"

#Add missing thread_title column
if ! head -n 1 "$datadir/discussions.tsv" | grep -P "\tthread_title$" > /dev/null; then
  ./add_thread_column.sh "$datadir/discussions.tsv" > "$datadir/discussions.tsv.new"
  mv -f "$datadir/discussions.tsv.new" "$datadir/discussions.tsv"
fi

# Extract discussions metrics from David's data
head -n 1 $thread_metrics_tree_string | iconv -f "iso8859-1" -t "UTF-8" > "$datadir/threads_metrics.tsv"
grep -P "^$pageid\t" $thread_metrics_tree_string | iconv -f "iso8859-1" -t "UTF-8" >> "$datadir/threads_metrics.tsv"

# Extract thread permalinks from David's data
head -n 1 $thread_titles | iconv -f "iso8859-1" -t "UTF-8" > "$datadir/threads_links.tsv"
grep -P "^$pageid\t" $thread_titles | iconv -f "iso8859-1" -t "UTF-8" >> "$datadir/threads_links.tsv"

# Extract actors from Erik's database
if [ ! -s "$datadir/actors.tsv" ]; then
  echo "SELECT e.canonical FROM element e LEFT JOIN element_edit ee ON ee.element_id = e.id LEFT JOIN section s ON ee.section_id = s.id LEFT JOIN revisions r ON ee.to_revision_id = r.id LEFT JOIN article_revisions ar ON ar.revision_id = r.id LEFT JOIN article a ON ar.article_id = a.id WHERE a.title = '$page' GROUP BY canonical ORDER BY canonical" | mysql -u $MYSQLUSER -p$MYSQLPASS $MYSQLDB > "$datadir/actors.tsv"
fi

# Match discussions with article sections and actors, and assemble all data into $datadir/threads_matched.csv and $datadir/actors_matched.csv
python match_discussions_sections.py "$page"

# UNUSED Collect HTML and screenshots for all revisions webpages 
# bash get_page_revisions.sh $page

