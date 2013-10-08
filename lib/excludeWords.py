#/usr/bin/python

@outputSchema("text:chararray") 
def excludeWords(textString,excludeFile):
	outList = []
	excludes = set(line.strip() for line in open(excludeFile))
	for word in textString.split():
		if word not in excludes:
			outList.append(word)
	return ' '.join(outList)

