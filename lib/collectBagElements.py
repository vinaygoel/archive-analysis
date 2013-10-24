#/usr/bin/python

@outputSchema("text:chararray") 
def collectBagElements(bag):
	elements = []
	for word in bag:
		elements.append(str(word[0]))
	return '\t'.join(elements)

