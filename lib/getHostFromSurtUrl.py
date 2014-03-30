import string

@outputSchema("text:chararray") 
def getHostFromSurtUrl(surtUrl):
   host = None
   if not surtUrl:
      return None
   surtUrlParts = surtUrl.split('/')
   surtHost = surtUrlParts[0]
   #strip away any text after whitespace
   surtHostParts = surtHost.split()
   if len(surtHostParts) == 0:
      return None
   surtHost = surtHostParts[0]
   #strip away port number
   surtHostParts = surtHost.split(':')
   surtHost = surtHostParts[0]        
   surtHost = remove_chars(surtHost)
   surtHostParts = surtHost.split(',')
   surtHostParts.reverse()
   host = '.'.join(surtHostParts)
   return host

def remove_chars(to_translate, translate_to=u''):
   chars_to_remove = u'()'
   translate_table = dict((ord(char), translate_to) for char in chars_to_remove)
   return to_translate.translate(translate_table)
