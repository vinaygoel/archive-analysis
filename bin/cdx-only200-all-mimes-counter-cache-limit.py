#!/usr/bin/env python
'''
Reads in sorted CDX lines and prints MIME-Year-Counts per SURT host
Skips non HTTP-200 captures, Robots.txt and DNS captures
warc/revisits are resolved to their parent MIME types
'''
import sys
import string
from datetime import datetime
import json
from collections import defaultdict
from collections import deque

def host_from_surt_url(surt_url):
    return surt_url.split(')')[0]

start_year = 1996
#end_year = 2017

prev_url = ""
prev_host = ""
url_digest_map = dict()
url_digest_queue = deque()
max_cache_length = 100
mime_year_result = defaultdict(dict)

for cdx_line in sys.stdin:
    cdx_line = cdx_line.rstrip('\n')
    cdx_parts = cdx_line.split(' ')
    if len(cdx_parts) != 11:
        continue
    (url,host,ts,orig,mime,rescode,digest) = (cdx_parts[0],
                                                   host_from_surt_url(cdx_parts[0]),
                                                   cdx_parts[1],
                                                   cdx_parts[2],
                                                   cdx_parts[3].lower(),
                                                   cdx_parts[4],
                                                   cdx_parts[5])
    try:
       ts_dt = datetime.strptime(ts, '%Y%m%d%H%M%S')
       year = ts_dt.year
       if year < start_year:# or year > end_year:
           continue
    except:
       continue

    #Skip DNS, robots, non 200 responses.
    if orig.startswith("dns:") or url.endswith("robots.txt") \
                               or (rescode != "200") \
                               or (mime.startswith('warc') and mime != 'warc/revisit'):
        pass
    else:
        key = digest
        if url != prev_url:
            if host != prev_host and prev_host != '':
                try:
                    print prev_host + "\t" + json.dumps(mime_year_result)
                except:
                    print prev_host + "\t" + "-"
                mime_year_result = defaultdict(dict)
	    url_digest_map = dict()
            url_digest_queue = deque()
            if mime != 'warc/revisit':
                url_digest_map[key] = mime
                url_digest_queue.append(key)
            mime_type = mime
        else:
	    mime_type = url_digest_map.get(key, None)
	    if mime_type is None:
	        #associate this mime with the url+digest
                if mime != 'warc/revisit':
	            url_digest_map[key] = mime
                    if len(url_digest_queue) >= max_cache_length:
                        #evict earliest digest
                        popped = url_digest_queue.pop()
                        url_digest_map.pop(popped, None)
                    if key not in url_digest_queue:
                        url_digest_queue.append(key)
	        mime_type = mime
        if mime_type == 'warc/revisit':
            mime_type = 'unresolved_revisit'
        old_value = 0
        try:
            old_value = mime_year_result[mime_type][year]
        except:
            old_value = 0
	mime_year_result[mime_type][year] = old_value + 1
        prev_url = url
        prev_host = host
try:
    print prev_host + "\t" + json.dumps(mime_year_result)
except:
    print prev_host + "\t" + "-"
