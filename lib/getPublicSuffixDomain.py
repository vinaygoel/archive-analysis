# use mozilla's public suffix list to get the top level domain. return 'other' for invalid input.
# Don't use: Pig is not able to set the path to the module
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

from publicsuffix import PublicSuffixList

@outputSchema("text:chararray") 
def getPublicSuffixDomain(host):
	if not host:
		return "other"
	psl = PublicSuffixList()
	domain = psl.get_public_suffix(host)
	if '.' not in domain:
        	domain = "other"
	return domain
