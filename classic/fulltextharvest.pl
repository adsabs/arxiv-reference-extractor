#!/usr/bin/perl

use strict;
use LWP::UserAgent;
use HTTP::Request ();
use HTTP::Response ();
use HTTP::Date ();
use POSIX qw(ceil);
use ADS::Environment;
use ADS::Abstracts::ArXiv;

my $script = $0; $script =~ s:^.*/::;
my $email = 'ads@cfa.harvard.edu';
my $timeout = 20;
my $agent = "ADS fulltext harvester (http://ads.harvard.edu; $email)";
my $baseurl = 'http://export.arxiv.org';
# this is purely for testing the program's logic (nothing gets downloaded)
#my $baseurl = 'http://adsduo.cfa.harvard.edu';

my $basedir = "$ENV{ADS_ABSTRACTS}/sources/ArXiv/fulltext";
my $format = '';
my $debug = 0;
my $list = 0;
my $nopdf = 0;

# set this to 1 or use the command line option '--refresh' to have
# *all* preprint checked for freshness
my $refresh = 0;

# delay between http requests to export
my $default_delay = 1;
my $delay = $default_delay;

my $usage = <<EOF
Usage: $script [OPTIONS] < arxiv_ids
This script downloads/refreshes the fullext of ArXiv preprints from
the top-level url $baseurl
into the directory $basedir
OPTIONS:
   --debug         print debugging information
   --delay NSEC    ensure that there are at least NSEC seconds between
                   download requests to the ArXiv server (default: $delay)
   --exclude FILE  list of preprints to exclude from harvesting; currently
                   this includes the preprints for which the source is not
                   available; http requests to these papers return a 403
   --list          list both existing files and newly downloaded ones
   --format FMT    download eprint in this particular format (usually 'pdf')
   --nopdf         do not automatically download a pdf version of the file
                   in addition to the 'default' version of the e-print
   --refresh       instead of skipping preprints that have been already
                   downloaded, perform a request to the server to verify
                   that we have the latest version for each one of them
                   (based on a GET If-Modified-Since HTTP request)
   --help          print this usage message and exit
EOF
    ;

my %enc2suff = ('x-gzip' => '.gz',
		''       => '');
my %type2suff = ('application/x-eprint-tar' => 'tar',
		 'application/postscript'   => 'ps',
		 'text/html'                => 'html',
		 'application/pdf'          => 'pdf',
		 'application/x-eprint'     => 'tex');

# now create all possible file extensions based on these
my @suffixes = map {
    my $s = $_; map { "$s$_" } values %enc2suff } values %type2suff;
#warn "suffixes: ", join(" ", @suffixes), "\n";
my $suffix_rx = join("|", map { "\Q$_\E" } @suffixes, 'gz');
#warn "suffix_rx is $suffix_rx\n";

my $ua = LWP::UserAgent->new(agent   => $agent,
			     timeout => $timeout,
			     from    => $email,
			     );
# last download time
my $ltime;
my %exclude;
chdir($basedir) or die "$script: cannot chdir $basedir: $!";

while (@ARGV and $ARGV[0] =~ /^--/) {
    my $s = shift(@ARGV);
    if ($s eq '--refresh') {
	$refresh = 1;
    } elsif ($s eq '--delay') {
	$delay = shift(@ARGV);
	die "$script: illegal value for option --delay: $delay"
	    unless ($delay > 0);
    } elsif ($s eq '--format') {
	$format = shift(@ARGV);
    } elsif ($s eq '--exclude') {
	my $ef = shift(@ARGV);
	die "$script: illegal value for option --exclude: $ef"
	    unless ($ef and -f $ef);
	open(my $fh, $ef) or
	    die "$script: cannot open input file $ef: $!";
	%exclude = map { s/\s+//g; ($_,1) } <$fh>;
    } elsif ($s eq '--debug') {
	$debug++;
    } elsif ($s eq '--list') {
	$list++;
    } elsif ($s eq '--nopdf') {
	$nopdf++;
    } elsif ($s eq '--help') {
	die $usage;
    } else {
	die "$script: unrecognized option '$s'\n", $usage;
    }
}

my ($r,$target,$file,$url,$eprintid);

while (<>) {
    my ($path,$rest) = split(/\s+/,$_,2);
    chomp($rest);
    $r = undef;
    $path =~ s/\.($suffix_rx)\Z//;
    # change id to path if id is given
    $path = "$1/$2" if ($path =~ /\A(\d{4})\.(\d{4,5})\Z/);
    my $eprint = ADS::Abstracts::ArXiv::parsepath($path);
    unless ($eprint) {
	warn "$script: illegal eprint id $path skipped\n";
	next;
    }
    $eprintid = $eprint->{id};
    next if ($exclude{$eprintid});
    $target = $eprint->{path};
    my $dir = $target;
    $dir =~ s:[^/]+\Z::g;
    my $type = $format || 'e-print';
    $url = "$baseurl/$type/$eprintid";
    unless (-d $dir) {
	if (system("mkdir -p $dir")) {
	    die "$script: error creating directory $dir: $?";
	} else {
	    warn "$script: created directory $dir\n";
	}
    }

    # see if there is already a file there
    $file = undef;
    if ($format) {
	$file = "$target.$format" if (-e "$target.$format");
    } else {
	foreach my $s (@suffixes) {
	    $file = "$target.$s" if (-e "$target.$s");
	}
    }
    if ($file) {
	warn "$script: $eprintid: $file exists already\n"
	    if ($debug);
	if ($refresh) {
	    warn "$script: $eprintid: checking file freshness\n"
		if ($debug);
	} else {
	    print $file, "\t", $rest, "\n" if ($list);
	    next;
	}
    } else {
	warn "$script: $eprintid: no local fulltext file found\n"
	    if ($debug);
    }

    $file ||= $target;
    warn "$script: $eprintid: performing download: getfile($ua,$url,$file,$format)\n"
	if ($debug);
    $r = getfile($ua,$url,$file,$format);
    if (not $r) {
	warn "$script: $eprintid: error downloading URL $url at ",
	scalar(localtime), "\n";
	next;
    } elsif ($r->code == 304) {
	warn "$script: $eprintid: file $file is up-to-date\n";
	next;
    } elsif (-e $file) {
	warn "$script: $eprintid: updated file $file\n";
    } else {
	warn "$script: $eprintid: downloaded file ", $r->{file}, "\n";
    }

    print $r->{file}, "\t", $rest, "\n";

} continue {

    my $fmt;
    if ($r and $r->{format}) {
	warn "$script: $eprintid: downloaded file in format ", $r->{format}, "\n"
	    if ($debug);
	$fmt = $r->{format};
    } elsif ($format) {
	$fmt = $format;
    } elsif (-e $file and $file =~ /\.pdf$/) {
	$fmt = 'pdf';
    }

    my $pdfurl = "$baseurl/pdf/$eprintid";
    my $download = 1;
    if ($nopdf) {
	$download = 0;
    } elsif ($fmt eq 'pdf') {
	# we've done this already
	$download = 0;
    } elsif ($pdfurl eq $url) {
	# done that
	$download = 0;
    } else {
	$download = 1;
    }
    warn "$script: $eprintid: now checking for pdf file\n" if ($debug);
    $file = "$target.pdf";
    if (-f $file) {
	warn "$script: $eprintid: $file exists already\n"
	    if ($debug);
	if ($refresh) {
	    warn "$script: $eprintid: checking file freshness\n"
		if ($debug);
	    $download = 1;
	} else {
	    $download = 0;
	}
    } else {
	warn "$script: $eprintid: no local pdf file found\n"
	    if ($debug);
    }
    # always download a pdf for this eprint
    if ($download) {
	warn "$script: $eprintid: now downloading pdf version: getfile($ua,$pdfurl,$file,'pdf')";
	$r = undef;
	$r = getfile($ua,$pdfurl,$file,'pdf');
	if (not $r) {
	    warn "$script: $eprintid: error downloading URL $pdfurl at ",
	    scalar(localtime), "\n";
	} elsif ($r->code == 304) {
	    warn "$script: $eprintid: file $file is up-to-date\n";
	} elsif (-e $file) {
	    warn "$script: $eprintid: updated file $file\n";
	} else {
	    warn "$script: $eprintid: downloaded file ", $r->{file}, "\n";
	}
    } else {
	warn "$script: $eprintid: skipping download of pdf file\n" if ($debug);
    }
}

sub throttle {
    my $delay = shift || $default_delay;

    my $ctime = time;
    my $delta = time - $ltime;
    if ($delta < $delay) {
	my $snooze = ceil($delay - $delta);
	warn "$script: sleeping $snooze seconds\n"
	    if ($debug);
	sleep($snooze);
    }
    $ltime = time;
}

# taken mostly from LWP::UserAgent::mirror()
#
sub getfile {
    my ($ua,$url,$file,$format) = @_;

    warn "$script: downloading document from $url\n" if ($debug > 1);
    my $request = HTTP::Request->new('GET', $url);
    if (-e $file) {
        my($mtime) = (stat($file))[9];
        if($mtime) {
            $request->header('If-Modified-Since' =>
                             HTTP::Date::time2str($mtime));
        }
    }
    my $nfile;
    my $tmpfile = "$file.tmp";
    throttle($delay);
    warn "$script: issuing request at ", scalar(localtime), ": $ua->request($request, $tmpfile)\n"
	if ($debug);
    my $response = $ua->request($request, $tmpfile);
#    my $response = undef;
#    return $response;

    my $outfmt;

    if ($response->is_success) {

	warn "$script: temporary file is $tmpfile\n" if ($debug > 1);
        my $file_length = (stat($tmpfile))[7];
        my($content_length) = $response->header('Content-length');

	my $base = $response->base;
	unless ($base) {
	    warn "$script: warning: $url: cannot find base url\n";
	    return undef;
	}
	my ($type) = $response->header('Content-Type');
	my ($encoding) = $response->header('Content-Encoding');
	my $suffix = $type2suff{$type} . $enc2suff{$encoding};
	$outfmt = $type2suff{$type};
	# my $suffix = ($format) ? ".$format" : '.html';
	if (not $suffix and $base =~ /\d+(\.[a-z\.]+)\Z/) {
	    $suffix = $1;
	} elsif ($suffix) {
	    $suffix = '.' . $suffix;
	}
	$nfile = $file;
	$nfile =~ s/\.[a-z\.]+\Z//;
	$nfile .= $suffix;

	if ($nfile ne $file and -f $file) {
	    warn "$script: warning: old file was $file, new file is $nfile\n";
	}

        if (defined $content_length and $file_length < $content_length) {
            unlink($tmpfile);
            warn "$script: error retrieving $file: " .
                "only $file_length out of $content_length bytes received\n";
	    return undef;
        } elsif (defined $content_length and $file_length > $content_length) {
            unlink($tmpfile);
            warn "$script: error retrieving $file: " .
                "expected $content_length bytes, got $file_length\n";
	    return undef;
        } else {
            # OK
            if (-e $file) {
                # Some dosish systems fail to rename if the target exists
                chmod 0777, $file;
                unlink $file;
            }
            unless (rename($tmpfile, $nfile)) {
                warn "$script: error downloading $file: cannot rename ",
		"'$tmpfile' to '$nfile': $!";
		return undef;
	    } else {
		warn "$script: destination file is $nfile\n" if ($debug);
	    }
#            if (my $lm = $response->last_modified) {
#                # make sure the file has the same last modification time
#                utime $lm, $lm, $file;
#            }
        }
    } elsif (-e $file and $response->code == 304) {
	# not modified
	return $response;
    } else {
	warn "$script: error downloading $url\n";
	warn "$script: ", $response->status_line if ($response);
        unlink($tmpfile);
	return undef;
    }

    $response->{format} = $outfmt;
    $response->{file} = $nfile;
    return $response;
}
