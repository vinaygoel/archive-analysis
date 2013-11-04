import string
@outputSchema("text:chararray") 
def removePunctuation(textString):
	remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
	textString = textString.translate(remove_punctuation_map)
	return textString
