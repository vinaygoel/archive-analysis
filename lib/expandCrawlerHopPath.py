@outputSchema("y:bag{t:tuple(id:chararray,pathString:chararray,pathLength:int)}")
def expandCrawlerHopPath(hoppathString):
	#hoppathString = "#1:CRAWLER#147452041:-#147502681:L#147440891:L#147440681:L#147439852:L#147445569:E"
	if hoppathString.startswith("##"):
		return None
	
	hopParts = hoppathString.split('#');
	
	#skip hopParts[0] and hopParts[1] (crawled head info)
	if len(hopParts) <= 2:
		return None
	ids = []
	paths = []
	for part in hopParts[2:]:
		splits = part.split(':')
		if len(splits) == 2:
			ids.append(splits[0])
			paths.append(splits[1])
	#direct link from crawler head node, so skip
	if len(ids) <= 1:
		return None
	#skip first part from the paths as it's the path from the crawler head node
	paths = paths[1:]
	#skip last part from the ids (as it's of the url in question)
	ids = ids[0:len(ids)-1]
	outBag = []
	for indexId, id in enumerate(ids):
		tup = (id, ''.join(paths[indexId:]), len(paths[indexId:]))
		outBag.append(tup)
	return outBag
