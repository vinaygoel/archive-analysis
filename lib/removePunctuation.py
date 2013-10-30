import string
punc = string.punctuation

@outputSchema("text:chararray") 
def removePunctuation(textString):
	str = list(textString)
	newStr = ''.join([o for o in str if not o in punc]).split()
	return ' '.join(newStr)

