# -*- coding: utf-8 -*-
import string

#removes stop words and punctuation
@outputSchema("text:chararray") 
def tokenize(textString,excludeFile):
   outList = []
   excludes = set(line.strip() for line in open(excludeFile))
      textString = translate_non_alphanumerics(textString)
   for word in textString.split():
      word = word.lower()
      if word not in excludes:
         outList.append(word)
   return ' '.join(outList)

def translate_non_alphanumerics(to_translate, translate_to=u' '):
   not_letters_or_digits = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~'
   translate_table = dict((ord(char), translate_to) for char in not_letters_or_digits)
   return to_translate.translate(translate_table)
