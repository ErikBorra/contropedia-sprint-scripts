#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, re
import csv, json
import urllib, htmlentitydefs
import time
from time import mktime, strptime
from datetime import date, datetime
from locale import setlocale, LC_ALL
setlocale(LC_ALL, 'en_GB.UTF-8')
import configparser
import StringIO

# read config file
ini_str = '[root]\n' + open('../../config.cfg', 'r').read() # add fake section
ini_fp = StringIO.StringIO(ini_str)
settings = configparser.RawConfigParser()
settings.readfp(ini_fp)

#
# *** PARAMETERS ***
#
#if True, messages about each match will be displayed
verbose = False
#if True, errors and warning messages will be displayed
display_errors = False
#if True, the values of some variables, etc, will be shown
debug = False
#size of the time window (in seconds) to consider an edit and a comment by the same user to cooccur in time
TIME_WINDOW = 600


# Open required data that was generated via the the generate_article_threads_data.sh
try:
    page_title = sys.argv[1]
    #datadir_discussions = "data/%s" % page_title
    datadir_discussions="%s/%s/discussions" % (settings.get('root','datadir'),page_title)
    os.chdir(datadir_discussions)
    with open('discussions.tsv') as csvf:
        discussions = list(csv.DictReader(csvf, delimiter="\t"))
    with open('revisions.tsv') as csvf:
        revisions = list(csv.DictReader(csvf, delimiter="\t"))
    with open('sections.tsv') as csvf:
        section_titles = csvf.read().split('\n')
    with open('threads_links.tsv') as csvf:
        links = list(csv.DictReader(csvf, delimiter="\t"))
    with open('threads_metrics.tsv') as csvf:
        metrics = list(csv.DictReader(csvf, delimiter="\t"))
    with open('revisions_sections.tsv') as csvf:
        rev_sec = csvf.read().split('\n')
        rev_sec.pop(0)
    with open('actors.tsv') as csvf:
        actors = csvf.read().split('\n')
        actors.pop(0)
    with open('actor_edits.tsv') as csvf:
        actor_edits = list(csv.DictReader(csvf, delimiter="\t"))
        #~ actor_edits.pop(0)
        
except Exception as e:
    sys.stderr.write("ERROR trying to read data")
    sys.stderr.write("%s: %s" % (type(e), e))
    sys.exit(1)


def safe_utf8_decode(t):
    try:
        return t.decode('utf-8')
    except:
        try:
            return t.decode('iso8859-1')
        except:
            return t


# Bunch of small functions and regexp to treat and cleanup dates and text
parse_ts = lambda t: date.isoformat(datetime.fromtimestamp(t))
#~ parse_date = lambda d: parse_ts(mktime(strptime(d.split(', ')[1].replace("(UTC)", "").strip(), "%d %B %Y")))
#~ convert_rev_ts = lambda s: time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
#~ convert_rev_ts_tool_labs = lambda s: time.strptime(s[:10], "%Y%m%d%H%M%S")
SPACES = ur'[  \s\t\u0020\u00A0\u1680\u180E\u2000-\u200F\u2028-\u202F\u205F\u2060\u3000]'
re_clean_blanks = re.compile(r'%s+' % SPACES)
clean_blanks = lambda x: re_clean_blanks.sub(r' ', x.strip()).strip()
re_entities = re.compile(r'&([^;]+);')
unescape_html = lambda t: clean_blanks(re_entities.sub(lambda x: unichr(int(x.group(1)[1:])) if x.group(1).startswith('#') else unichr(htmlentitydefs.name2codepoint[x.group(1)]), safe_utf8_decode(t)).encode('utf-8'))
re_talk = re.compile(r'\[\[Talk:.*#([^\|]+)\|?.*\]\]')
re_abstract = re.compile(r'(^|\W)(intro(duction)?|abstract|lead|summar(y|ies)|preamble|headers?)(\W|$)', re.I)
clean_thread_name = lambda t: unescape_html(t).replace('_', ' ').strip('"[]()«»!?~<>.= ').strip("'")
re_clean_lf = re.compile(r'\s*<LF>\s*', re.I)
re_clean_text = re.compile(r'[^\w\d]+')
re_clean_spec_chars = re.compile(r'[^\w\d\s]')
clean_text = lambda t: re_clean_text.sub(' ', re_clean_lf.sub('', unescape_html(t))).lower().strip()
re_text_splitter = re.compile(r"[^\w\d']+")
is_null_col = lambda x: not x or x in ["", "0", "-1"]

def convert_rev_ts(s): 
	if len(s)<10: print 'bad date:', s
	date = None
	try: 
		date = time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
	except Exception as e:
		try: 
			date = time.strptime(s, "%Y-%m-%d %H:%M:%S")
		except Exception as e:
			try: 
                            date = time.strptime(s[:12], "%Y%m%d%H%M%S")
                        except Exception as e:
                            print 'ERROR parsing date: %s' % s
	return date

def time_match(rev_ts, comment_ts, TIME_WINDOW):
	if debug: print rev_ts, comment_ts, TIME_WINDOW, ' -> ', int(mktime(convert_rev_ts(rev_ts))), comment_ts, (abs(int(mktime(convert_rev_ts(rev_ts))) - comment_ts) <= TIME_WINDOW)
	if (abs( int(mktime(convert_rev_ts(rev_ts))) - comment_ts) <= TIME_WINDOW): return True
	else: return False	

# Prepare threads data from all discussions lines
thread = None
threads = []
curthread = ""
threadidx = {}
#~ comments_by_timestamp = {}
user_comments = {}
n_comments = 0
# Read data from David's discussions file line by line
for row in discussions:
    # Skip lines without a thread title
    if not row['thread_title']:
        continue
    # Store in threads array previous thread object and create a new one whenever reaching a line with a different thread title
    idx = len(threads)
    th = clean_thread_name(row['thread_title'])
    if th != curthread:
        curthread = th
        if thread:
            threads.append(thread)
        thread = {"index": idx,
                  "name": th,
                  "rawname": row['thread_title'].strip('=[] '),
                  "date_min": "",
                  "users": [],
                  "nb_users": 0,
                  "messages": [],
                  "nb_messages": 0,
                  "users_hindex": 0,
                  "max_depth": 0,
                  "tree_hindex": 0,
                  "chains_num": 0,
                  "chains_comments": 0,
                  "fulltext": "",
                  "timestamped_text": [],
                  "permalink": "",
                  "revisions": [],
                  "article_sections": [],
                  "match": 0,
                  "revisions_coocc": [],
                  "article_sections_coocc": [],
                  "match_coocc": 0,
                  "comments": {}
                  }
        threadidx[th.lower()] = idx
    if is_null_col(row["timestamp"]):
        curthread = th
        continue
    # Collect and compute useful metas on the threads
    dt = parse_ts(int(row['timestamp'])*60)
    if 'date_min' not in thread:
        thread['date_min'] = dt
    else:
        thread['date_min'] = min(thread['date_min'], dt)
    if 'date_max' not in thread:
        thread['date_max'] = dt
    else:
        thread['date_max'] = max(thread['date_max'], dt)
    us = row["author_name"].strip()
    if us not in thread["users"]:
        thread['users'].append(us)
        thread['nb_users'] += 1
    thread['nb_messages'] += 1
    thread['messages'].append(row)
    # Save a field containing the concatenated cleaned up text from all comments
    thread['fulltext'] += " " + clean_text(row["text"])
    # And one as an array of tuples (text, timestamp) for each comment for use in the actors matching part
    thread['timestamped_text'].append((clean_text(row["text"]), int(row['timestamp'])*60, int(row['id'])))
    #Save comment (with thread structure)
    comment_id = int(row["id"])
    parent_id = int(row["parent_id"])
    thread['comments'][comment_id] = {'parent': parent_id, 'children': [], 'date': dt, 'ts': int(row['timestamp'])*60, 'author': row['author_name'], 'text': row['text']}
    if parent_id in thread['comments']:
        thread['comments'][parent_id]['children'].append(str(comment_id))
    #~ #create a record of all the comments, sorted by timestamp (with timestamp as the dictionary key)
    #~ if row['timestamp'] not in comments_by_timestamp:
        #~ comments_by_timestamp[row['timestamp']] = []
    #~ comments_by_timestamp[row['timestamp']] = (row["author_name"], curthread)
    #create a record of all the comments
    comment_author = row["author_name"]
    comment_ts = int(row['timestamp'])*60
    if comment_author not in user_comments:
        user_comments[comment_author] = []
    user_comments[comment_author].append((comment_ts, comment_id, idx))		
    n_comments += 1   
    
# Save last current thread since we won't find a new one after it
if thread:
    threads.append(thread)
print '%d threads and %d comments read' % (len(threads), n_comments)

# Complete threads with their permalinks
n_permalinks = 0
for row in links:
    t = clean_thread_name(row['thread_title']).lower()
    if t in threadidx:
        threads[threadidx[t]]['permalink'] = "http://en.wikipedia.org/wiki/%s#%s" % (row['talk_page'], urllib.quote(threads[threadidx[t]]['rawname'].replace(' ', '_')).replace('%', '.'))
        n_permalinks += 1
    else:
        if display_errors: sys.stderr.write("ERROR: could not match one thread from links: %s\n" % t)

# Complete threads with David's precomputed metrics
n_metrics = 0
for row in metrics:
    t = clean_thread_name(row['thread_title']).lower()
    if t in threadidx:
        for f in ["users_hindex", "max_depth", "tree_hindex", "chains_num", "chains_comments"]:
            threads[threadidx[t]][f] = int(row[f])
        n_metrics += 1
    else:
        if display_errors: sys.stderr.write("ERROR: could not match one thread from metrics: %s\n" % t)

print '%d threads completed with permalink (%d%%) and %d with thread metrics (%d%%)' % (n_permalinks, (n_permalinks+1)*100/(len(threads)+1), n_metrics, (n_metrics+1)*100/(len(threads)+1))

revisions_sec = {}
# Look for revisions referencing a thread as comment
for row in rev_sec:
    if not row:
        continue
    rev_id, sec_title = row.split('\t')
    rev_id = int(rev_id)
    if not rev_id in revisions_sec:
        revisions_sec[rev_id] = []
    revisions_sec[rev_id].append(sec_title)
print '%d revisions associated to some section' % len(revisions_sec)

# Save actors involved in each revision
rev_actors = {}
for row in actor_edits:
    if not row:
        continue	
    rev_id = int(row['revision_id'])    
    if rev_id not in rev_actors:
        rev_actors[rev_id] = []
    rev_actors[rev_id].append(row['actor'])
print '%d revisions associated to some actor' % len(revisions_sec)

#Loop through all revisions searching matches with comments 
#(explicit mentions of comments in the edit summary, or cooccurrence in time of comments by the same user)
n_revs = 0
n_revs_comments = 0
n_revs_comments_secs = 0
#~ n_revs_comments_actors = 0
n_cooccs = 0
n_cooccs_secs = 0
n_cooccs_actors = 0

actor_matches_coocc = {}
for row in revisions:
    n_revs += 1
	#search for a blurry version of the thread title within the revision comment
    src = re_talk.search(row["rev_comment"])
    if src:
        t = clean_thread_name(src.group(1)).lower()
        if t in threadidx:
            thread_id = [threadidx[t]]
            rev_id = int(row['rev_id'])
            if verbose: print "MATCH FOUND (EDIT SUMMARY):", row["rev_id"], t
            n_revs_comments += 1
            threads[threadidx[t]]['revisions'].append(rev_id)
            try:
                threads[threadidx[t]]['article_sections'] += revisions_sec[rev_id]
                n_revs_comments_secs += 1
            except:
                if display_errors: sys.stderr.write('WARNING: revision %s could not be found in the correspondance list of revisions/sections\n' % rev_id)
            threads[threadidx[t]]['match'] += 1
    
            
    #look for cooccurrences with comments by the same users at the same time (time window size for the match depends on parameter "TIME_WINDOW")
    if row["rev_user"] in user_comments:
        for (comment_ts, comment_id, thread_id) in user_comments[row["rev_user"]]:
            if time_match(row["rev_timestamp"], comment_ts, TIME_WINDOW):
                if verbose: print "MATCH FOUND (USER TIME COOCCURRENCE):", row["rev_user"], row["rev_id"], row["rev_comment"], '/', t
                threads[thread_id]['revisions_coocc'].append(rev_id)
                n_cooccs += 1
                try:
                    threads[thread_id]['article_sections_coocc'] += revisions_sec[rev_id]
                    n_cooccs_secs += 1
                except:
                    if display_errors: sys.stderr.write('WARNING: revision %s could not be found in the correspondance list of revisions/sections\n' % rev_id)
                threads[thread_id]['match_coocc'] += 1

                #save cooccurrences of comments with edits, and the corresponding actors
                if rev_id in rev_actors:
                    for a in rev_actors[rev_id]:
                        if a not in actor_matches_coocc:
                            actor_matches_coocc[a] = {}
                        if thread_id not in actor_matches_coocc[a]:
                            actor_matches_coocc[a][thread_id] = {'comment_ids':[],'timestamps':[]}
                        actor_matches_coocc[a][thread_id]['comment_ids'].append(comment_id)
                        actor_matches_coocc[a][thread_id]['timestamps'].append(comment_ts)
                        if verbose: print "ACTOR / COMMENT MATCH FOUND (USER TIME COOCCURRENCE):", a, '/', t, ' (', threads[thread_id]['comments'][comment_id]['text'], ')'
                        n_cooccs_actors += 1

                    #~ if comment_id not in actor_matches_coocc[a][thread_id]:
                            #~ actor_matches_coocc[a][thread_id][comment_id] = ('comment_ids':[],'timestamps':[])	
                    #~ actor_matches_coocc[a][thread_id][0]comment_id].append(rev_id)	

print '%d total revisions' % n_revs
print '%d revisions matched to comments via edit summary, and %d of these to some section' % (n_revs_comments, n_revs_comments_secs) 
print '%d cooccurrences found (edits and comments by the same user in a time window of %d seconds). %d of these were matched to some section' % (n_cooccs, TIME_WINDOW, n_cooccs_secs) 
print '%d comment/actor matches based on cooccurrences' % n_cooccs_actors

# Look for article sections within thread names and fulltext of all comments
sections = {}
allsections = ""
# First generate a blurry cleaned list of the article's section titles
for section in section_titles:
    s = clean_thread_name(section).lower()
    if s not in sections:
        sections[s] = section
    allsections += " | " + s
    # Try to match the sections titles within the fulltext of each thread's comment, might be imperfect
    # so doing it onlty for long thread names since too short ones will most probably match many false positives
    if len(s) > 5:
        for t in threads:
            try:
                re_match_s = re.compile(r"%s" % re_clean_spec_chars.sub(".?", s))
            except:
                if display_errors: print "ERROR compiling regexp %s %s" % (s, re_clean_spec_chars.sub(".?", s))
                continue
            # Only validate when the word was found in at least half of the thread's comments
            if 2*len(re_match_s.findall(t['fulltext'])) > t['nb_messages']:
                if verbose: print "MATCH maybe FOUND:", t['name'], "/", section
                t['article_sections'].append(section)
                t['match'] += 1
# Then try to find sections titles within the thread's title
for thread in threadidx:
    # If a thread's title matches a section one, this is definitely a match
    if thread in sections:
        if verbose: print "TITLE MATCH FOUND:", thread, "/", sections[thread]
        threads[threadidx[thread]]['article_sections'].append(sections[thread])
        threads[threadidx[thread]]['match'] += 1
    # Otherwise try some heuristic when finding the section within a thread's title
    # Only take it when the section's name is longer than 3 chars to avoid false positives,
    # And only take it when the section has at least 2 words or a tenth of the number of words in the thread's title
    else:
        for section in sections:
            n_words = len(re_text_splitter.split(section))
            if section in thread and 3 < len(section) and (n_words > 1 or 10 * n_words > len(re_text_splitter.split(thread))):
                if verbose: print "MATCH probably FOUND:", thread, "/", sections[section]
                for test in threads[threadidx[thread]]['article_sections']:
                    tmps = clean_thread_name(section).lower()
                    # If we find a bigger match than a previous one, we favor this one
                    if test in tmps and test != tmps:
                        if verbose: print " -> probably better than match with « %s », removing it" % test
                        threads[threadidx[thread]]['article_sections'].remove(test)
                        threads[threadidx[thread]]['match'] -= 1
                threads[threadidx[thread]]['article_sections'].append(sections[section])
                threads[threadidx[thread]]['match'] += 1
    # Quite often threads correspond to the header of a wikipage following a bunch of possible names for it (abstract, summary, etc...)
    # Try to match those
    if re_abstract.search(thread):
        if verbose: print "MATCH probably GUESSED:", thread, "/", "abstract"
        threads[threadidx[thread]]['article_sections'].append("asbtract")
        threads[threadidx[thread]]['match'] += 1


# IDEAS FOR MATCH IMPROVEMENTS:
# - use userids and timestamps of comments to countermatch with same user's revisions around the same period of time

matches = sum([1 for t in threads if t['match'] > 0])
print "=================="
print "FOUND %d matches out of %d threads (%s)" % (matches, len(threadidx), str(matches*100/len(threadidx))+"%")
print "=================="
for t in threads:
    if not 'max_depth' in t:
        th = clean_thread_name(t['name']).lower()
        print "WARNING Can't find max_depth in %s" % th
    if False and not t['match']:
        print "MISSING:", t['name'], t['nb_messages'], t['nb_users']

#Save the threads data for debug purposes
#~ with open('threads.json', 'w') as jsonf:
    #~ json.dump(threads, jsonf, ensure_ascii=False)

# Save the built data on each article/thread match as a csv
make_csv_line = lambda arr: ",".join(['"'+str(a).replace('"', '""')+'"' if ',' in str(a) else str(a) for a in arr])
headers = ["article_title", "section", "thread", "controversiality", "min_date", "max_date", "nb_users", "nb_messages", "users_hindex", "max_depth", "tree_hindex", "chains_num", "chains_comments", "permalink"]
with open('threads_matched.csv', 'w') as csvf:
    print >> csvf, make_csv_line(headers)
    for t in threads:
        if not t['nb_users']*t['nb_messages']:
            continue
        data = [page_title, "", t['rawname'], "TBD", t['date_min'], t['date_max'], t['nb_users'], t['nb_messages'], t["users_hindex"], t["max_depth"], t["tree_hindex"], t["chains_num"], t["chains_comments"], t['permalink']]
        if len(t['article_sections']):
            for s in t['article_sections']:
                data[1] = s
                print >> csvf, make_csv_line(data)
        else:
            print >> csvf, make_csv_line(data)

# Identify page's actors within threads
make_csv_line = lambda arr: "\t".join([str(a) for a in arr])
headers = ["article_title", "actor", "thread", "thread_permalink", "actor_in_thread_title", "n_matches_in_thread", "comments_timestamps", "comments_ids", "n_cooccs_in_thread", "cooccs_timestamps", "cooccs_ids"]
headers2 = ["article_title", "actor", "thread", "thread_permalink", "comment_text", "comment_date", "comment_timestamp", "comment_author", "comment_id", "comment_parent_id", "comment_children_ids", "actor_in_comment", "actor_in_previous_comments", "actor_in_thread_title", "n_matches_in_thread", "actor_coocc_comment", "actor_coocc_previous_comments", "n_cooccs_in_thread", "n_comments_in_thread"]

with open('actors_matched.csv', 'w') as csvf, open('actors_matched_comments.csv', 'w') as csvf2:
    print >> csvf, make_csv_line(headers)
    print >> csvf2, make_csv_line(headers2)
    matches = 0
    cooccs_num = 0
    # Iterate on all of the page's actors as identified within Eric's database
    for actor in actors:
        # build a regexp to blurry match words similar to the actor by replacing with ".?" every non alphanumeric (or space) character
        act = clean_thread_name(actor).lower()
        re_actor = re.compile(r"%s" % re_clean_spec_chars.sub(".?", act))
        # SKIP empty actors and single-letter ones such as "d"
        if len(act) < 2: continue
        # Iterate on all threads to search the actor
        for thread in threads:
            thread_id = thread['index']
            # Search for the actor in the thread's title at first
            match_title = 1 if len(re_actor.findall(thread['name'].lower())) else 0
            # Search for the actor in each comment, sum the matches and list the corresponding timestamps
            all_matches = 0
            timestamps = []
            ids = []
            for te, ti, cid in thread["timestamped_text"]:
                n_match = len(re_actor.findall(te.lower()))
                if n_match:
                    all_matches += n_match
                    timestamps.append(ti)
                    ids.append(cid)
            # If there's at least one match, dump a tsv line
            if (match_title or all_matches or (actor in actor_matches_coocc and thread_id in actor_matches_coocc[actor])) and thread['permalink']:
                if (actor in actor_matches_coocc and thread_id in actor_matches_coocc[actor]): 
					cooccs = actor_matches_coocc[actor][thread_id]
					if verbose: print 'writing ACTOR THREAD MATCH: ', actor, '/', threads[thread_id]['name']
                else: cooccs = {'comment_ids':[],'timestamps':[]}		 
                print >> csvf, make_csv_line([page_title, actor, thread['rawname'], thread['permalink'], all_matches, match_title, timestamps, ids, len(cooccs['comment_ids']), cooccs['timestamps'], cooccs['comment_ids']])
                cooccs_num += len(cooccs['comment_ids'])
                #~ first_ts = 0 if match_title else timestamps[0]
                matches += 1
                # Write on a second tsv file a line for each comment belonging to a thread that has been matched with the actor
                for c in thread['comments']:
                    comm = thread['comments'][c]
                    match_comment = 1 if c in ids else 0
                    already_matched = 1 if (timestamps and comm['ts']>int(timestamps[0])) else 0	
                    
                    #~ match_comment_coocc = 0		
                    #~ already_matched_coocc = 0
                    match_comment_coocc = 1 if c in cooccs['comment_ids'] else 0
                    already_matched_coocc = 1 if (len(cooccs['timestamps']) and comm['ts']>int(cooccs['timestamps'][0])) else 0	
                    #~ if actor in actor_matches_coocc and thread_id in actor_matches_coocc[actor]: 
					#	#~ if c in actor_matches_coocc[actor][thread_id]['comment_ids']: match_comment_coocc = 1 
					#	#~ if (comm['ts']>int(actor_matches_coocc[actor][thread_id]['timestamps'][0])): already_matched_coocc = 1 	
                    try:									
                    	print >> csvf2, make_csv_line([page_title, actor, thread['rawname'], thread['permalink'], comm['text'], comm['date'], comm['ts'], comm['author'], c, comm['parent'], '|'.join(comm['children']), match_comment, already_matched, match_title, len(ids), match_comment_coocc, already_matched_coocc, len(cooccs['comment_ids']), len(thread['comments'])])
                    except Exception as e:
                    	if display_errors: sys.stderr.write("ERROR trying to write comment matches")
                    	if display_errors: sys.stderr.write("%s: %s" % (type(e), e))
						
    print '%d actors and %d threads -> %d actor-thread matches found' %(len(actors), len(threads), matches)
    print 'cooccurrences of actors and threads -> %d' %cooccs_num

    if debug:
		for a in actor_matches_coocc:
			print a, actor_matches_coocc[a]
