#!/usr/bin/env python

# modified CDX_Writer code
from hanzo.warctools import ArchiveRecord #from https://github/internetarchive/warctools

import os
import re
import sys
import base64
import hashlib
import urllib
import urlparse
from optparse  import OptionParser

class WARC_Metadata_Parser(object):
    # init()
    #___________________________________________________________________________
    def __init__(self, file, parseType):

        #self.parseType_map = {'hopinfo': 'origCrawledUrl date origViaUrl hopPathFromVia sourceTag',
         #                 'outlinks': 'origUrl date origOutlinkUrl linktype linktext'
         #                }

        self.file   = file
        self.parseType = parseType
        self.crlf_pattern = re.compile('\r?\n\r?\n')

        #these fields are set for each record in the warc
        self.offset        = 0
        self.headers       = None
        self.content       = None

    # get_original_url() 
    #___________________________________________________________________________
    def get_original_url(self, record):
        url = record.url
        # Some arc headers contain urls with the '\r' character, which will cause
        # problems downstream when trying to process this url, so escape it.
        # While we are at it, replace other newline chars.
        url = url.replace('\r', '%0D')
        url = url.replace('\n', '%0A')
        url = url.replace('\x0c', '%0C') #formfeed
        url = url.replace('\x00', '%00') #null may cause problems with downstream C programs
        return url

    # get_date() 
    #___________________________________________________________________________
    def get_date(self, record):
	return record.date
   
    # get_hopinfo() 
    #___________________________________________________________________________
    def get_hopinfo(self, record):
	contentString = record.content[1]
	contentLines = contentString.split('\r\n')
	viaUrl = "-"
	viaPath = "-"
	sourceTag = "-"
	origUrl = self.get_original_url(record)
	date = self.get_date(record)
	for line in contentLines:
		if line.startswith("hopsFromSeed"):
			splits = line.split(' ')
			if len(splits) > 1:
				viaPath = splits[1]
		elif line.startswith("via"):
			splits = line.split(' ')
			if len(splits) > 1:
				viaUrl = splits[1]
		elif line.startswith("sourceTag"):
			splits = line.split(' ')
			if len(splits) > 1:
				sourceTag = splits[1]
	result=origUrl + '\t' + date + '\t' + viaUrl + '\t' + viaPath + '\t' + sourceTag
	return result  

    #outlink: http://obama.senate.gov/i==r.length-1 X =JS_MISC
    #'outlinks': 'origUrl date origOutlinkUrl linktype linktext' 
    # get_outlinks() 
    #___________________________________________________________________________
    def get_outlinks(self, record):
	contentString = record.content[1]
	contentLines = contentString.split('\r\n')
	origUrl = self.get_original_url(record)
	date = self.get_date(record)
	links = []
	for line in contentLines:
		if line.startswith("outlink"):
			splits = line.split(' ')
			if len(splits) > 3:
				origOutlinkUrl = splits[1]
				linktype = splits[3]
				linkString = origUrl + '\t' + date + '\t'+ origOutlinkUrl + '\t' + linktype + '\t' #no anchor text
				links.append(linkString)
	if len(links) > 0:
		return '\n'.join(links)  
	
 
    # split_headers_and_content()
    #___________________________________________________________________________
    def parse_headers_and_content(self, record):
        """Returns a list of header lines, split with splitlines(), and the content.
        We call splitlines() here so we only split once, and so \r\n and \n are
        split in the same way.
        """
        if 'metadata' == record.type:
            try:
                headers, content = self.crlf_pattern.split(record.content[1], 1)
            except ValueError:
                headers = record.content[1]
                content = None
            headers = headers.splitlines()
        else:
            headers = None
            content = None

        return headers, content


    # parse_metadata()
    #___________________________________________________________________________
    def parse_metadata(self):
        
	fh = ArchiveRecord.open_archive(self.file, gzip="auto", mode="r")
        for (offset, record, errors) in fh.read_records(limit=None, offsets=True):
            self.offset = offset

            if record:
                if record.type != 'metadata':
                        continue
                ### precalculated data that is used multiple times
                self.headers, self.content = self.parse_headers_and_content(record)

		result = None
		if (self.parseType == "hopinfo"):
			result = self.get_hopinfo(record)
		elif (self.parseType == "outlinks"):
			result = self.get_outlinks(record)
		else:
			sys.exit("Invalid parseType option: " + self.parseType)
		if result:
			print result	
            elif errors:
                sys.exit("Exiting with the following errors:\n" + str(errors))
            else:
                pass # tail
        fh.close()

# main()
#_______________________________________________________________________________
if __name__ == '__main__':

    parser = OptionParser(usage="%prog [options] warc.gz")
    parser.set_defaults(parseType = "hopinfo")
    parser.add_option("--parseType",  dest="parseType", help="hopinfo or outlinks. [default: '%default']")
    
    (options, input_files) = parser.parse_args(args=sys.argv[1:])

    if not 1 == len(input_files):
        parser.print_help()
        exit(-1)

    warc_metadata_parser = WARC_Metadata_Parser(input_files[0], options.parseType)
    warc_metadata_parser.parse_metadata()
