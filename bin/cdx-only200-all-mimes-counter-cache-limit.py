#!/usr/bin/env python
'''
Mime-Year Counter (JSON output)
Reads in sorted CDX lines, Skips non HTTP-200 captures, robots.txt and DNS captures
warc/revisits are resolved to their parent MIME types
'''
import sys
import string
from datetime import datetime
import json
from collections import defaultdict
from collections import deque

prev_url = ""
url_digest_map = dict()
url_digest_queue = deque()
max_cache_length = 100
mime_year_result = defaultdict(dict)

for cdx_line in sys.stdin:
    cdx_line = cdx_line.rstrip('\n')
    cdx_parts = cdx_line.split(' ')
    if len(cdx_parts) < 11:
        continue
    (url,ts,orig,mime,rescode,digest) = (cdx_parts[0],
                                         cdx_parts[1],
                                         cdx_parts[2],
                                         cdx_parts[3].lower().split(';')[0],
                                         cdx_parts[4],
                                         cdx_parts[5])
    #Skip bad timestamps
    try:
        ts_dt = datetime.strptime(ts, '%Y%m%d%H%M%S')
        year = ts_dt.year
    except:
        continue

    #Skip dns, robots, whois
    if orig.startswith("dns:") or url.startswith('whois://') or url.endswith("robots.txt") \
                               or (mime.startswith('warc') and mime != 'warc/revisit'):
        continue

    key = digest
    if url != prev_url:
        url_digest_map.clear()
        url_digest_queue.clear()

    if mime != 'warc/revisit' and rescode == '200':
        url_digest_map[key] = mime
        #evict earliest digest if exceeded cache length
        if len(url_digest_queue) >= max_cache_length:
            popped = url_digest_queue.pop()
            url_digest_map.pop(popped, None)
        if key not in url_digest_queue:
            url_digest_queue.append(key)

    mime_type = url_digest_map.get(key, None)

    if mime_type is None:
        # still unresolved, or non HTTP-200, so skip
        prev_url = url
        continue
    try:
        old_value = mime_year_result[mime_type][year]
    except:
        old_value = 0
    mime_year_result[mime_type][year] = old_value + 1
    prev_url = url
try:
    print(json.dumps(mime_year_result))
except:
    print("FAILED")
