#! /usr/bin/env perl
#
# This short script is derived from `extractrefs.pl`. As the name implies, it
# does a reference extraction for a single PDF file. It's invoked by the Python
# "classic" compatibility code since it doesn't make sense to Python-ify this
# whole workflow, which will hopefully be replaced by much cleverer tools soon.

use strict;
use warnings;
use ADS::References::Document::Parser;

my $script = $0; $script =~ s:^.*/::;
my $debug = 0;
my $help = 0;

while (@ARGV and $ARGV[0] =~ /^--/) {
    my $s = shift(@ARGV);

    if ($s eq '--debug') {
        $debug++;
    } elsif ($s eq '--help') {
        $help++;
    } else {
        &usage();
    }
}

my $pfile = shift(@ARGV);  # this is the PDF
my $tfile = shift(@ARGV);  # the target (output) reference file
my $bibcode = shift(@ARGV);

warn "$script: debugging set at level $debug\n" if ($debug);
&usage if ($help || !defined($bibcode));

my $doc_parser = ADS::References::Document::Parser->new(debug => $debug);

exit(process_one_pdf($pfile, $tfile, $bibcode));


sub usage {
    die <<EOF
Usage: $script [OPTIONS] PDF-PATH TARGET-REF-PATH BIBCODE

OPTIONS:
  --debug  Print debugging information
  --help   Print this help

EOF
;
}


sub process_one_pdf {
    my $file    = shift;
    my $reffile = shift;
    my $bibcode = shift;

    my $content = $doc_parser->content($file);
    unless ($content) {
        warn "$script: $file: cannot extract content from $file\n";
        return 1;
    }

    my @references;
    eval {
        @references = $doc_parser->parse($content);
    };

    if ($@) {
        warn "$script: $file: error extracting references from pdf file\n";
    } else {
        warn "$script: $file: found ", scalar(@references), " references\n"
            if ($debug);
    }

    if (scalar(@references) == 0) {
        warn "$script: $file: warning: no references found\n";
        return 1;
    } elsif (scalar(@references) < 4) {
        warn "$script: $file: warning: only ", scalar(@references), " references found\n";
        return 1;
    }

    my $fh;
    unless (open($fh, ">$reffile")) {
        warn "$script: $file: cannot open output file $reffile: $!";
        return 1;
    }

    print $fh "%R $bibcode\n%Z\n", join("\n", @references), "\n";
    return 0;
}
