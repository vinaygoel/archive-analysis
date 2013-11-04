#removes stop words and punctuation
import string
@outputSchema("text:chararray") 
def tokenize(textString,excludeFile):
	outList = []
	excludes = set(line.strip() for line in open(excludeFile))
	remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
	for word in textString.split():
		word = word.lower()
		word = word.translate(remove_punctuation_map)
		if word not in excludes:
			outList.append(word)
	return ' '.join(outList)
