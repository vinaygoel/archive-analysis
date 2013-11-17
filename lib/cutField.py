@outputSchema("text:chararray") 
def cutField(textString,separator,fieldNum):
	if not textString:
		return None
	textStringParts = textString.split(separator)
	if len(textStringParts) > fieldNum:
		return textStringParts[fieldNum]
	return None
