# -*- coding: utf-8 -*-

@outputSchema("text:chararray")
def compressWhiteSpaces(textString):
        outList = []
        for word in textString.split():
        	outList.append(word)
        return ' '.join(outList)
