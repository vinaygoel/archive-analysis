#!/usr/bin/env perl
#read in space delimited sorted lines
#print only one line per first field (key)
$prev = "";
while(<STDIN>) {
	$line = $_;
	chomp($line);
	($first,@rest) = split(/ /,$line);
	if($first eq $prev) {
		next;
	}
	print "$first @rest\n";
	$prev = $first;
}
