#! /usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Cwd;
use File::Basename;
use ADS::Environment;
use ADS::Abstracts::Biblooker;
use ADS::Abstracts::ArXiv;
use ADS::References::Document::Parser;
use ADS::Abstracts::Entities;

# As per Thorsten's recipe of 2012-01-20
use constant {
    CUTOVER2016  => 20170209, # 1486670400, # Date::Parse::str2time('2017-02-09T20:00', 'GMT')
    CUTOVER2011  => 20111206, # 1323129600, # Date::Parse::str2time('2011-12-06', 'GMT')
    CUTOVER2009  => 20091231, # 1262217600, # Date::Parse::str2time('2009-12-31', 'GMT')
    CUTOVER2006  => 20061102, # 1162425600, # Date::Parse::str2time('2006-11-02', 'GMT')
    CUTOVER2004  => 20040101, # 1072915200, # Date::Parse::str2time('2004-01-01', 'GMT')
    CUTOVER2003  => 20030101, # 1041379200, # Date::Parse::str2time('2003-01-01', 'GMT')
    CUTOVER2002  => 20020901, # 1030838400, # Date::Parse::str2time('2002-09-01', 'GMT')
    PATHTL2016   => '/texlive/2016/bin/arch',
    PATHTL2011   => '/texlive/2011/bin/arch',
    PATHTL2009   => '/texlive/2009/bin/arch',
    PATHTETEX3   => '/3/bin',
    PATHTETEX2   => '/2/bin',
    TEXMFCNF2004 => '/2/texmf-2004/web2c',
    TEXMFCNF2003 => '/2/texmf-2003/web2c',
    TEXMFCNF2002 => '/2/texmf-2002/web2c'
};

my $script = $0; $script =~ s:^.*/::;
my $def_texbase  = "$ENV{ADS_ABSTRACTS}/sources/ArXiv/teTeX";
my $def_pbase = "$ENV{ADS_ABSTRACTS}/sources/ArXiv/fulltext";
my $def_tbase = "$ENV{ADS_REFERENCES}/sources";
my $try_pdf = 1;
my $harvest_pdf = 1;
my $skip_refs = 0;

my ($texbase, $pbase, $tbase, $debug, $force, $help);

while (@ARGV and $ARGV[0] =~ /^--/) {
    my $s = shift(@ARGV);

    if ($s eq '--pbase') {
        $pbase = shift(@ARGV);
    } elsif ($s eq '--tbase') {
        $tbase = shift(@ARGV);
    } elsif ($s eq '--texbase') {
        $texbase = shift(@ARGV);
    } elsif ($s eq '--debug') {
        $debug++;
    } elsif ($s eq '--force') {
        $force++;
    } elsif ($s eq '--no-pdf') {
        $try_pdf = 0;
    } elsif ($s eq '--no-harvest') {
        $harvest_pdf = 0;
    } elsif ($s eq '--skip-refs') {
        $skip_refs = 1;
    } elsif ($s eq '--help') {
        $help++;
    } else {
        &usage();
    }
}

warn "$script: debugging set at level $debug\n" if ($debug);
&usage if ($help);

$texbase ||= $def_texbase;
$pbase ||= $def_pbase;
$tbase ||= $def_tbase;
$debug ||= 0;
$force ||= 0;

# this is the encoding in which output text files are
# created by pdftotext; it must be understood both by
# pdftotext and ADS::Abstracts::Entities::Encoder
my $pdfenc = 'ASCII7';

my $looker = ADS::Abstracts::Biblooker->new;
my $doc_parser = ADS::References::Document::Parser->new(debug => $debug);
#my $encoder = ADS::Abstracts::Entities::Encoder->new(Encoding => $pdfenc,
#                 Format => 'Text');
my $status = 0;
my $tmp_dir = "$ENV{ADS_TMP}/ArXiv.$$";
my $keep_tmp = 1 if ($debug > 1);
my $thisdir = getcwd();
my $bindir = $0; $bindir =~ s:/[^/]+$::;
my $origpath = $ENV{PATH};
my $dirname = dirname(__FILE__);

my %spliteprints = &GetSplitEprints();
my $spliteprintsrx = join('|', keys(%spliteprints));

while (<>) {
    my ($pre,$bibcode,$accno,$subdate) = split;
    my $eprint = ADS::Abstracts::ArXiv::parsepath($pre);

    unless ($eprint) {
        warn "$script: cannot parse eprint $pre\n";
        $status++;
        next;
    }

    my $suffix = $eprint->{suffix};
    my $pfile = $pre;

    unless (-f $pfile) {
        $pfile = "$pbase/" . $eprint->{path} .".$suffix";
    }

    $suffix = lc($suffix);
    my $id = $eprint->{id};
    my $year = $eprint->{year};

    unless (-f $pfile) {
        warn "$script: $id: cannot find \"$pfile\"\n";
        $status++;
        next;
    }

    my $format;

    if ($suffix eq 'tar.gz' or $suffix eq 'tar' or
        $suffix eq 'tex.gz' or $suffix eq 'tex' or $suffix eq 'gz') {
        $format = 'tex';
    } elsif ($suffix eq 'pdf' or $suffix eq 'pdf.gz') {
        $format = 'pdf';
    } else {
        warn "$script: $id: dont know how to extract refs for $pre\n";
        $status++;
        next;
    }

    my $tfile = "$tbase/". $eprint->{path} .".raw";

    if (not -f $tfile) {
        warn "$script: $id: creating $tfile\n" if ($debug);
    } elsif (-M $tfile > -M $pfile) {
        warn "$script: $id: $tfile needs updating\n" if ($debug);
    } elsif ($force) {
        warn "$script: $id: forcing recreation of $tfile\n" if ($debug);
    } else {
        warn "$script: $id: $tfile is up-to-date\n" if ($debug);
        next;
    }

    unless ($bibcode and $subdate) {
        my $partial = ADS::Abstracts::ArXiv::id2bib($id);
        chop($partial);
        warn "$script: $id: partial bibcode is $partial\n" if ($debug);
        my @match = $looker->lookbib($partial);

        unless (@match) {
            warn "$script: $id: could not find bibcode matching \"$partial\"\n";
            $status++;
            next;
        }

        $bibcode = $match[0];
        $subdate = $match[3];
        warn "$script: $id: bibcode set to $bibcode, subdate set to $subdate\n";
    }

    if ($format eq 'tex') {
        my $err = process_one_tex($pfile, $tfile, $bibcode, $id, $subdate);
        chdir($thisdir);

        if ($err) {
            warn "$script: $id: failed to process LaTeX source file \"$pfile\"\n";

            if ($try_pdf) {
                $format = 'pdf';
                warn "$script: $id: will attempt to extract references from PDF version\n";
            }
        }

        if ($try_pdf) {
            # now retrieve a PDF file for the preprint
            $pfile = "$pbase/" . $eprint->{path} .".$format";

            if ($harvest_pdf) {
                warn "$script: $id: harvesting pdf fulltext: $bindir/fulltextharvest.pl --format pdf\n";
                open(my $fh, "| $bindir/fulltextharvest.pl --format pdf");
                print $fh $eprint->{path}, "\n";
                undef($fh);
                warn "$script: $id: retrieved PDF file $pfile\n"
                    if (-f $pfile);
            } elsif (-f $pfile) {
                warn "$script: $id: using PDF file $pfile\n";
            } else {
                warn "$script: $id: no PDF file found, skipping\n";
                $status++;
                next;
            }
        }
    }

    if ($format eq 'pdf') {
        my $err = process_one_pdf($pfile, $tfile, $bibcode, $id);

        if ($err) {
            warn "$script: $id: error processing PDF file $pfile\n";
            $status++;
            next;
        }
    }

    print "$pfile\t$tfile\n";
}

warn "$script: finished processing at ", scalar(localtime(time)), "\n";
warn "$script: $status files could not be processed\n"
    if ($status);
exit(0);


sub usage {
    die <<EOF
Usage: $script [OPTIONS] < filelist

OPTIONS:
  --pbase DIR    Specify alternative base directory for fulltext source
                 (default: $def_pbase)
  --tbase DIR    Specify alternative base directory for target ref files
                 (default: $def_tbase)
  --texbase DIR  Specify alternative directory for tetex tree
                 (default: $def_texbase)
  --force        force recreation of references even if target file exists
                 and is more recent than source
  --no-harvest   do not attempt to harvest or refresh PDF files from arXiv
  --no-pdf       do not attempt to process PDF files if origina source
                 was latex (implies --no-harvest)
  --skip-refs    perform all data processing but skip writing the references
                 to file (to avoid causing reprocessing); often used with --debug to test
  --debug        print debugging information

The program reads from stdin a table consisting of the fulltext e-print
file (first column) and optionally its corresponding bibcode (second
column), accno number (third column), and submission date (fourth column).
If a bibcode is not given, one is obtained from bib2accno.list

The fulltext filenames typically are in one of these forms:
   arXiv/0705/0161.tar.gz
   arXiv/0705/0160.pdf
   math/2006/0604548.tex.gz
EOF
;
}


sub process_one_pdf {
    my $file    = shift;
    my $reffile = shift;
    my $bibcode = shift;
    my $id = shift;

    my $content = $doc_parser->content($file);
    unless ($content) {
        warn "$script: $id: cannot extract content from $file\n";
        return 1;
    }

    my @references;
    eval {
        @references = $doc_parser->parse($content);
    };

    if ($@) {
        warn "$script: $id: error extracting references from pdf file\n";
    } else {
        warn "$script: $id: found ", scalar(@references), " references\n"
            if ($debug);
    }

    if (scalar(@references) == 0) {
        warn "$script: $id: warning: no references found\n";
        return 1;
    } elsif (scalar(@references) < 4) {
        warn "$script: $id: warning: only ", scalar(@references), " references found\n";
        return 1;
    }

    if ($skip_refs) {
        warn "$script: $id: Skipping writing references to $reffile\n"
            if ($debug);
        return 0;
    }
    warn "$script: $id: writing references to $reffile\n" if ($debug);

    my $dir = $reffile; $dir =~ s:[^/]+\Z::;
    unless (-d $dir) {
        system("mkdir -p $dir");
    }

    my $fh;
    unless (open($fh, ">$reffile")) {
        warn "$script: $id: cannot open output file $reffile: $!";
        return 1;
    }

    print $fh "%R $bibcode\n%Z\n", join("\n", @references), "\n";
    return 0;
}

sub process_one_tex {
    my $file    = shift;
    my $reffile = shift;
    my $bibcode = shift;
    my $id = shift;
    my $subdate = shift;
    my $def_bibitem = 'bibitem';
    my $count = 0;
    my $ignore = 0;

    if (-d $tmp_dir) {
        warn "$script: $id: directory $tmp_dir exists already; deleting it\n";
        cleanup_tmp($tmp_dir, 1);
    }

    # Change to work directory. Create if necessary.
    # All work will be done in this directory and afterwards
    # everything will be cleaned up

    if (!-d $tmp_dir){
        warn "$script: $id: temp directory does not exist. Creating: $tmp_dir\n"
            if ($debug);
        mkdir($tmp_dir);
    }

    my $inputfile = $file;
    $inputfile =~ s:^.*/::;

    warn "$script: $id: Copying source file to work directory:\n$file --> $tmp_dir/$inputfile\n"
        if ($debug);

    # Copy the source over to the work directory  for unpacking, processing
    if (system("cp -f \"$file\" \"$tmp_dir/$inputfile\"")) {
        warn "$script: $id: error running cp -f \"$file\" \"$tmp_dir/$inputfile\"\n";
        cleanup_tmp($tmp_dir,1);
        return 1;
    }

    warn "$script: $id: Changing to directory: $tmp_dir\n"
        if ($debug);

    unless(chdir($tmp_dir)) {
        warn "$script: $id: cannot cd to $tmp_dir: $!";
        return 1;
    }

    my $error = 0;

    if ($inputfile =~ /\.tar\.gz$/ or $inputfile =~ /\.tgz$/) {
        $error = system("tar xzf $inputfile") and
            warn "$script: $id: error running tar xzf $inputfile";
    } elsif ($inputfile =~ /\.tar$/) {
        $error = system("tar xf $inputfile") and
            warn "$script: $id: error running tar xf $inputfile";
    } elsif ($inputfile =~ /\.tex\.gz$/ or $inputfile =~ /\.gz$/) {
        $error = system("gunzip $inputfile") and
            warn "$script: $id: error running gunzip $inputfile";
    } elsif ($inputfile =~ /\.tex$/) {
        # already a tex file -- do nothing
    } else {
        # assume it's a plain tex file
        $error = system("mv -f \"$inputfile\" \"$inputfile.tex\"") and
            warn "$script: $id: error moving file $inputfile to $inputfile.tex";
        $inputfile .= '.tex';
    }

    return 1 if ($error);

    # Determine a list of files of interest
    use File::Find;
    my @files;

    find(sub {
        my $f = $File::Find::name;
        $f =~ s:^\./::;
        push(@files,$f) if (-f $f);
    }, '.');

    if (scalar(@files) == 0) {
        cleanup_tmp($tmp_dir);
        warn "$script: $id: TeX/LaTeX file seems to be missing!\n";
        return 1;
    }

    # as of teTeX v.3 both types of output (pdf or dvi)
    # are possible from 'latex', so to simplify our life
    # we just force pdf output for those papers and deal
    # with parsing references out of the marked up pdf
    my $output_fmt = 'pdf';

    # Thorsten's recipe of 2012-01-20
    if ($subdate >= CUTOVER2016) {
        $ENV{TEXMFCNF} = '';
        $ENV{PATH} = "$texbase/" . PATHTL2016 . ":$dirname:$origpath";
    } elsif ($subdate >= CUTOVER2011) {
        $ENV{TEXMFCNF} = '';
        $ENV{PATH} = "$texbase/" . PATHTL2011 . ":$dirname:$origpath";
    } elsif ($subdate >= CUTOVER2009) {
        $ENV{TEXMFCNF} = '';
        $ENV{PATH} = "$texbase/" . PATHTL2009 . ":$dirname:$origpath";
    } elsif ($subdate >= CUTOVER2006) {
        $ENV{TEXMFCNF} = "$texbase/3/texmf/web2c";
        $ENV{PATH} = "$texbase/". PATHTETEX3 . ":$dirname:$origpath";
    } else {
        $ENV{PATH} = "$texbase/". PATHTETEX2 . ":$dirname:$origpath";

        if ($subdate >= 20040101) {
            $ENV{TEXMFCNF} = "$texbase/2/texmf-2004/web2c";
        } elsif ($subdate >= 20030101) {
            $ENV{TEXMFCNF} = "$texbase/2/texmf-2003/web2c";
        } elsif ($subdate >= 20020901){
            $ENV{TEXMFCNF} = "$texbase/2/texmf-2002/web2c";
        } else {
            $ENV{TEXMFCNF} = "$texbase/2/texmf/web2c";
        }
    }

    # Determine which file is the main tex file
    my @main = find_main(@files);
    unless (@main) {
        warn "$script: $id: no main file found!\n";
        return 1;
    }

    my $title = $main[0]->{title} || '';
    my $fmt   = $main[0]->{fmt}   || 'tex';

    # if title is very short we may have mis-computed it
    $main[0]->{title} = '' if (length($title) < 10);

    # Now it's time to introduce markup that will help locating
    # the references later on
    foreach my $main (@main) {
        my $orig = $main->{file};
        $main->{bibitem} ||= $def_bibitem;
        next unless (-f $orig);
        next if ($orig =~ /(psfig)/);

        warn "$script: $id: Adding markup. Processing: $orig\n"
            if ($debug);
        munge_refs($orig,$main->{bibitem},$output_fmt) or
            warn "$script: $id: error adding ref markup to file $orig\n";
    }

    # now try getting references
    my @references = get_references($id,$fmt,$output_fmt,@main);

    # if we couldn't get them, see if changing the source .tex to
    # include pdf files helps
    unless (@references) {
        my $changed = 0;

        foreach my $main (@main) {
            my $orig = $main->{file};
            next unless (-f $orig);
            next if ($orig =~ /(psfig)/);
            warn "$script: $id: converting .eps to .pdf in source file $orig\n"
                if ($debug);
            $changed += convertps2pdf($orig);
        }

        if ($changed) {
            warn "$script: $id: attempting to re-extract references\n"
                if ($debug);
            @references = get_references($id,$fmt,$output_fmt,@main);
        }
    }

    if (@references and $skip_refs) {
        warn "$script: $id: Skipping writing references to $reffile\n"
            if ($debug);
    } elsif (@references) {
        warn "$script: $id: Writing references to: $reffile\n"
            if ($debug);

        my $dir = $reffile; $dir =~ s:[^/]+\Z::;
        unless (-d $dir) {
            system("mkdir -p $dir");
        }

        open(my $rf, ">$reffile");
        unless ($rf) {
            warn "$script: $id: cannot open ref file $reffile: $!";
            cleanup_tmp($tmp_dir);
            return 1;
        }
        print $rf "%R $bibcode\n%Z\n", @references;
        undef($rf);
    }

    # Clean up all files in the working directory
    warn "$script: $id: Cleaning up the mess behind us.  All files in $tmp_dir will be unlinked\n"
        if ($debug);
    cleanup_tmp($tmp_dir);

    if (@references) {
        return 0;
    } else {
        return 1;
    }
}


# converts tex source to call .pdf rather than .ps file when
# including graphics
sub convertps2pdf {
    my $orig = shift;
    my $ofh;

    local $/ = undef ;
    open($ofh,"<$orig");
    unless ($ofh) {
        warn "$script: cannot open file $orig: $!";
        return 0;
    }
    $a = <$ofh> || '';
    close($ofh);

    # if there is nothing to change in source, just return
    return 0 unless ($a =~ s/\.(ps|eps|epsi|epsf)\b/.pdf/sg);

    my $nfh;
    my $tmpfile = "$orig.tmp";
    open($nfh,">$tmpfile");
    unless ($nfh) {
        warn "$script: cannot open file $tmpfile: $!";
        return 0;
    }
    print $nfh $a;
    close($nfh);

    if (system("mv -f \"$tmpfile\" \"$orig\"")) {
        warn "$script: error copying file $tmpfile to $orig: $@";
        return 0;
    } else {
        warn "$script: updated file $orig\n" if ($debug);
        return 1;
    }
}


sub get_references {
    my $id = shift;
    my $fmt = shift;
    my $output_fmt = shift;
    my @references;

    foreach my $main (@_) {
        my $ignore   = $main->{ignore};
        my $mtexfile = $main->{file};
        my $bibitem  = $main->{bibitem};
        my $mbname   = $main->{basename};
        my $title    = $main->{title};

        # If the preprint was withdrawn, the flag 'ignore' will be 'true'
        if ($ignore) {
            warn "$script: $id: file $mtexfile withdrawn!  Skipping it...\n";
            next;
        }

        my $texcmmd;
        if ($output_fmt eq 'pdf') {
            $texcmmd = ($fmt eq 'tex') ? 'pdftex' :
                'pdflatex -interaction=nonstopmode';
        } else {
            $texcmmd = ($fmt eq 'tex') ? 'tex' :
                'latex -interaction=nonstopmode';
        }

        if ($debug) {
            warn "$script: $id: Bibitem definition: $bibitem\n";
            warn "$script: $id: Command: $texcmmd\n";
            warn "$script: $id: Main text file: $mtexfile\n";
            warn "$script: $id: Main base name: $mbname\n";
            warn "$script: $id: Paper title: $title\n";
        }

        my $out_file = "$mbname." . $output_fmt;
        my $tex_log  = "$mbname.log";

        # Compile the source
        if ($debug) {
            warn "$script: $id: PATH=", $ENV{PATH}, "\n";
            warn "$script: $id: TEXMFCNF=", $ENV{TEXMFCNF}, "\n";
            warn "$script: $id: Compiling TeX source...\n";
        }

        if (SystemTimeout(100, $texcmmd, $mtexfile)) {
            warn "$script: $id: execution of '$texcmmd $mtexfile' failed\n";
        } elsif ($debug) {
            warn "$script: $id: finished compiling TeX source\n"
        }

        my $output = find_output($tex_log);
        unless ($output) {
            warn "$script: $id: cannot deduct output file from log\n";
            warn "$script: $id: going with: $out_file and hoping for the best\n";
            $output = $out_file;
        }

        if ($output and $output ne $out_file) {
            warn "$script: $id: expected output file to be \"$out_file\", ",
            "but found file \"$output\"\n";
        }

        if (not -f $output) {
            warn "$script: $id: file $output not found, skipping it\n";
            next;
        } elsif (0 == -s $output) {
            warn "$script: $id: file $output has zero size, removing it\n";
            unlink($output);
            next;
        } elsif ($debug) {
            warn "$script: $id: output file is $output\n";
        }

        my $text_output = $output . '.txt';

        if ($output =~ /\.dvi\Z/) {
            if (system("dvitype $output < /dev/null > $text_output")) {
                warn "$script: $id: error running dvitype on file $output: $@\n";
                next;
            }
            @references = find_dvi_references($text_output,$title);
        } else {
            if (system("pdftotext -raw -enc $pdfenc $output $text_output")) {
                warn "$script: $id: error running pdftotext on file $output: $@\n";
                next;
            } elsif ($debug) {
                warn "$script: $id: successfully run \"pdftotext -raw -enc $pdfenc $output $text_output\"\n";
            }
            @references = find_pdf_references($text_output,$title);
        }

        if (@references) {
            warn "$script: $id: found ", scalar(@references), " refs in file $text_output\n";
            last;
        } else {
            warn "$script: $id: Cannot find any references in $text_output!\n";
        }
    }

    return @references
}


sub find_output {
    my $file = shift or return undef;
    open(my $fh, $file) or return undef;
    while (<$fh>) {
        return "$1" if (/^Output written on (\S+)/i);
    }
    return undef;
}

# cleans up references extracted from PDF
sub clean {
    my $input     = shift;
    my $title     = shift || '';
    my $type      = 0;
    my $rtype     = undef;

    $input =~ s/^\s+//;
    $input =~ s/\s+$//;
    $input =~ s/\s\s+/ /g;

    if ($title and $input =~ s/\Q$title\E\s*\d*\s?//) {
        warn "removing title: $title\n"
            if ($debug);
    }

    # this is found in AAS latex
    $input =~ s/\|{2,}\{,/---, /;

    if (!defined($rtype)) {
        $input =~ s/^\d+ (\d+)/$1/;
        $input =~ s/^\d+\s*([\[\(]\s*\d+)/$1/;
        $input =~ s/^\d+?(1\.)/$1/;
        $input =~ s/^\d\d+\s*([A-Z])/$1/;
        $input =~ s/^(0|[2-9])\s*([A-Z])/$2/;

        if ($input =~ /^([\[\(])\s*\d+\s*([\]\)])/) {
            $rtype = 2; # [1] Author, I. 2004, foo
        } elsif ($input =~ /^([\[\(])\s*[\w.]{1,10}\s*([\]\)])/) {
            $rtype = 2; # [Au] Author, I. 2004, foo
        }  elsif ($input =~ /^\d+(\W+)/) {
            $rtype = 1; # 1. Author, I. 2004, foo  -- or -- 1] Author, I. 2004, foo
        }
    } else {
        my $type = $rtype;
    }

    $input =~ s/\s*-\s*/-/g;

    if ($type == 2) {
        $input =~ s/^\d+\s*(\[\s*\d+\s*\])/$1/;
    } elsif ($type == 1) {
        $input =~ s/^\d+\s+(\d+)/$1/;
    } else {
        $input =~ s/^\d+\s*([a-z])/$1/i;
    }

    $input =~ s/\\([A-Z])(?=[^\"]+\")/\"$1/g;
    $input =~ s/\s\s+/ /g;
    $input =~ s/ ,/,/g;
    return $input;
}

# extracts references from a pdftxt file
sub find_pdf_references {
    my $textfile = shift;
    my $title = shift || '';

    local $/ = undef;
    open(my $dh, $textfile);
    unless ($dh) {
        warn "$script: cannot open file $textfile!\n";
        return ();
    }
    my $buff = <$dh>;
    close($dh);

    my @references;
    while ($buff =~ m{\G.*?<r>(.*?)<[ ]*/r[ ]*>}gsc) {
        my $r = $1;
        # $r = $encoder->encode($r);
        $r =~ s/\-\n//g;
        $r =~ s{\b($spliteprintsrx)/(\d{7})}{$spliteprints{$1}/$2}g;
        $r =~ s/\s+/ /g;
        $r =~ s/^\s+|\s+$//g;
        push(@references,"$r\n");
    }

    return @references;
}


# extracts references from a dvitxt file
sub find_dvi_references {
    my $textfile = shift;
    my $title = shift;

    # New we're going to process the ASCII file with DVI commands
    my @references = ();
    my $reference = '';

    open(my $dh,$textfile);
    unless ($dh) {
        warn "$script: cannot open file $textfile!\n";
        return ();
    }

    while(<$dh>) {
        next unless (/\bcitation_open\b/);
    }

    while (<$dh>) {
        if (/\bref_close\b/) {
            $reference = &clean($reference,$title);
            push(@references, "$reference\n");
            last;
        }

        if (/\bcitation_open\b/) {
            $reference = &clean($reference,$title);
            push(@references, "$reference\n");
            $reference = '';
            next;
        }

        next unless (/^\[/);
        next if (/^\[(References|REFERENCES|Bibliography|BIBLIOGRAPHY)\]$/x);
        chomp;
        s/^\[//;
        s/\]$//;

        if ($reference =~ /-\s*$/) {
            s/^\s+//;
            # it looks like Edwin was trying to catch
            # the tail-end of preprint codes here,
            # but why is he overriding the reference?
            # I'm assuming it was a typo -- AA 1/13/06
            if (/^[a-z]+[ \/]+\d{7}/){
                # $reference = $_;
                $reference =~ s/\s*$//;
                $reference .= $_;
            } else {
                $reference =~ s/\s*-\s*$//;
                $reference .= $_;
            }
        } else {
            $reference .= $_;
        }
    }

    return @references;
}

# modifies the contents of the tex/latex files to include
# markup used to extract references.
# If the output format is to be a pdf file, markup is added
# straight into the file as XML tags
# If the output is a dvi file, we instead use tex's \special
# command so we can parse it out of the dvitext output
sub munge_refs {
    my $orig = shift;
    my $bibitem = shift;
    my $format = shift;
    my $convertps = shift;
    my $newfile = "$orig.tmp";

    # here we attempt to do our best at marking start and end of
    # references while preserving references as intact as possible;
    # this means preventing TeX from doing formatting which will
    # then need to be undone in an expensive way.
    # Here is what we do:
    #   1. put markers at the beginning and end of a references,
    #      which will be used later on to extract it from the
    #      output pdf file
    #   2. prevent page breaks in the middle of a reference by
    #      having each one start at the beginning of a page
    #   3. disable hyphenation as much as possible
    my $opnref = ($format eq 'pdf') ? '$<$references$>$' :
        '\special{ref_open} ';
    my $clsref = ($format eq 'pdf') ? '$<$/references$>$' :
        ' \special{ref_close}';
    my $opntag = ($format eq 'pdf') ? '\newpage\onecolumn\section*{}$<$r$>$\sloppy\raggedright' :
        '\special{citation_open} ';
    my $clstag = ($format eq 'pdf') ? '$<$/r$>$' :
        ' \special{citation_close}';
    # my $convertps = ($format eq 'pdf') ? 1 : 0;

    my ($ofh,$nfh);
    open($ofh,"<$orig");
    unless ($ofh) {
        warn "$script: cannot open input file $orig: $!";
        return 0;
    }

    open($nfh,">$newfile");
    unless ($nfh) {
        warn "$script: cannot open output file $newfile: $!";
        return 0;
    }
    my $open_ref = 0;
    my $count = 0;

    while (<$ofh>) {
        print $nfh $_;
        last if (/\\begin\s*\{(chapthebibliography|thebibliography|references)\}/i);
    }

    # check if we've gotten to the end of the file without finding
    # the start of the reference section, in which case, reopen the file again
    # and try to tag references
    if (eof($ofh) and $orig =~ /\.(bib|bbl)$/) {
        warn "$script: could not find start of references, but attempting to tag bibitems anyway for bbl file";
        close($ofh);
        close($nfh);
        open($ofh,"<$orig");
        unless ($ofh) {
            warn "$script: cannot open input file $orig: $!";
            return 0;
        }
        open($nfh,">$newfile");
        unless ($nfh) {
            warn "$script: cannot open output file $newfile: $!";
            return 0;
        }
    }

    my $lastref = '';
    my $tag = '';
    my $type = '';

    while (<$ofh>) {
        next if (/^\s*\%/ or /^\s*$/);
        s/\n//;

        if (/^\s*\\end\s*\{(chapthebibliography|thebibliography|references)\}/i) {
            if ($lastref) {
                print $nfh "\n", tag_ref($tag, $lastref, $opntag, $clstag, $type);
                $lastref = '';
            }

            print $nfh "\n$_\n";
            last;
        }

        s/\b(\w+\s*)--(\s*\w+)\b/$1-$2/g;

        if (s/^\s*\\(bibitem|reference|rn|rf|rfprep|item|\Q$bibitem\E)\b//i) {
            unless ($tag) {
                $tag = $1;
                if ($tag eq 'bibitem' or $tag eq $bibitem) {
                    $type = 'bibitem';
                } elsif ($tag eq 'reference' or $tag eq 'ref') {
                    $type = 'reference';
                }
            }

            if ($lastref) {
                print $nfh "\n", tag_ref($tag, $lastref, $opntag, $clstag, $type);
                $lastref = '';
            }

            $count++;
            $lastref = $_;
        } elsif ($tag) {
            # we have seen the first bibliographic item
            $lastref .= "\n" . $_;
        } else {
            # still in the pre-reference list area
            print $nfh "$_\n";
        }
    }

    if ($lastref) {
        print $nfh "\n", tag_ref($tag, $lastref, $opntag, $clstag, $type);
        $lastref = '';
    }

    print $nfh "\n";
    print $nfh (<$ofh>);

    close($ofh);
    close($nfh);

    warn "$script: tagged $count references in file $orig\n"
        if ($debug);

    if (system("mv -f \"$newfile\" \"$orig\"")) {
        warn "$script: error copying file $newfile to $orig: $@";
        return 0;
    }

    # Replace all {\em <text>} by "<text>".
    # Some papers use "\em" for titles
    warn "$script: Replacing all \"{\\em <text>}\" with <text> in file $orig\n"
        if ($debug);

    local $/ = undef;
    my $tmpfile = "$orig.tmp";

    open($ofh,"<$orig");
    unless ($ofh) {
        warn "$script: cannot open file $orig: $!";
        return 0;
    }

    open($nfh,">$tmpfile");
    unless ($nfh) {
        warn "$script: cannot open file $tmpfile: $!";
        return 0;
    }

    $a = <$ofh> || '';
    # why is this useful? To attempt to normalize all titles in double quotes. YMMV
    $a =~ s/\{\\em\ (.*?)\}/"$1"/sg;
    $a =~ s/\{\\it\ (.*?)\}/"$1"/sg;
    $a =~ s/\\textit\{(.*?)\}/"$1"/sg;
    $a =~ s/\\emph\{(.*?)\}/"$1"/sg;
    # translate incorporation of postscript to pdf graphics
    $a =~ s/\.(ps|eps|epsi|epsf)\b/.pdf/sg if ($convertps);
    print $nfh $a;
    close($nfh);
    close($ofh);

    if (system("mv -f \"$tmpfile\" \"$orig\"")) {
        warn "$script: error copying file $newfile to $orig: $@";
        return 0;
    }

    # if the output format is pdf, convert all encapsulated
    # postscript files to pdf because pdflatex will look for those
    # in many (but not all) cases when the source tex uses macros
    # such as \includegraphics{}
    if ($format eq 'pdf') {
        use File::Find;
        find(sub { eps2pdf($_, '.') if (/\.(ps|eps|epsi|epsf)\Z/); }, '.');
    }

    return 1;
}


# ps/eps -> pdf conversion
sub eps2pdf {
    my $file = shift;
    my $pdf = $file;

    $pdf =~ s/\.(ps|eps|epsi|epsf)\Z/.pdf/ or return 1;
    return 1 if (-f $pdf);

    warn "$script: running epstopdf $file >/dev/null 2>/dev/null\n"
        if ($debug);

    if (SystemTimeout(5, 'epstopdf', $file)) {
        warn "$script: error converting file $file to $pdf\n";
        return 0;
    } else {
        return 1;
    }
}


# decomposes a reference in a prefix macro and text part,
# munges the text part and inserts opening and closing tags around it
sub tag_ref {
    my $prefix = shift;
    my $text = shift;
    my $open = shift;
    my $close = shift;
    my $type = shift;

    use Text::Balanced qw( extract_bracketed );
    my ($arg, $rest, $sep);

    if ($type eq 'bibitem') {
        # can have one of two forms:
        # \bibitem{bar}
        # \bibitem[foo]{bar}
        ($arg, $rest, $sep) = extract_bracketed($text, '[]');
        if ($arg) {
            $prefix .= $sep . $arg;
            $text = $rest;
        }
        ($arg, $rest, $sep) = extract_bracketed($text, '{}');
        if ($arg) {
            $prefix .= $sep . $arg;
            $text = $rest;
        }
    } elsif ($type eq 'reference') {
        # this can be either:
        #    \reference{bibcode} this is the ref (emulateapj)
        # or:
        #    \reference this is the ref (all others)
        ($arg, $rest, $sep) = extract_bracketed($text, '{}');
        if ($arg) {
            $prefix .= $sep . $arg;
            $text = $rest;
        }
    }

    my $ref = "\\". $prefix . " $open " . remove_diacritics($text) . "\n$close";
    return $ref;
}


# removes accents that trip up pdftotext (as of v. 3.02);
# recipe based on http://www.giss.nasa.gov/tools/latex/ltx-401.html
#    *  \`{o} produces a grave accent
#    * \'{o} produces an acute accent
#    * \^{o} produces a circumflex
#    * \"{o} produces an umlaut or dieresis
#    * \H{o} produces a long Hungarian umlaut
#    * \~{o} produces a tilde
#    * \c{c} produces a cedilla
#    * \={o} produces a macron accent (a bar over the letter)
#    * \b{o} produces a bar under the letter
#    * \.{o} produces a dot over the letter
#    * \d{o} produces a dot under the letter
#    * \u{o} produces a breve over the letter
#    * \v{o} produces a "v" over the letter
#    * \t{oo} produces a "tie" (inverted u) over the two letters
sub remove_diacritics {
    my $string = shift;

    # M{\"u}ller
    $string =~ s/\{\\[\`\'\^\"\~\=\.]([a-zA-Z])\}/$1/g;
    # M\"{u}ller
    $string =~ s/\\[\`\'\^\"\~\=\.Hcbduvt]\{([a-zA-Z])\}/$1/g;
    # M\"uller
    $string =~ s/\\[\`\'\^\"\~\=\.]([a-zA-Z])/$1/g;

    return $string;
}

# given a list of tex files, finds which one is the main one;
# we do this by analyzing the tex/latex commands in it and
# seeing if one file includes some other one
sub find_main {
    my @candidates;
    my $texfile;
    my $ignore = 0;
    my %notmain;

    # list of basenames for sample files distributed with Latex macros for
    # different journals.  We assign a negative score to each one of them
    # reflecting how sure we feel that they are not the main latex file
    my %exclude = (
        'mn2eguide'   => -100,
        'mn2esample'  => -100,
        'mnras_guide' => -100,
        'aa'          => -100,
        'new_feat'    => -50,
        'rnaas'       => -5,
        'mnras_template' => -2    # some people put their paper in this file!
    );

    foreach my $infile (@_) {
        my $format = '';
        my $score = 0;
        my $title;
        my $bibitem = '';

        if ($infile =~ /(psfig)/) {
            next;
        } elsif ($infile =~ /\.(tex|ltx|latex|revtex)$/i) {
            $score++;
        } elsif ($infile =~ /\.(bib|bbl)$/i) {
            # probably just the bibliography
        } elsif ($infile =~ /\.txt$/ or $infile =~ /^[^\.]+$/) {
            # weird ending
        } else {
            next;
        }

        if ($infile =~ m/\.TEX$/) {
            my $oldfile = $infile;
            $infile =~ s/\.TEX$/.tex/;
            rename($oldfile,$infile);
        }

        my $basename = $infile;
        $basename =~ s/\.\w+$//;
        if ($exclude{$basename}) {
            $score += $exclude{$basename};
        }

        warn "$script: processing file: $infile\n"
            if ($debug > 1);

        open(my $fh, "<$infile");
        unless ($fh) {
            warn "$script: cannot open file $infile: $!";
            next;
        }

        while (<$fh>) {
            if (/\%auto\-ignore/) {
                $ignore++;
                last;
            }

            if (/^\s*\\begin\s*\{document\}/ or
                /^\s*[^%].*?\\begin\s*\{document\}/ or
                /^\s*\\documentclass\b/ or
                /^\s*\\documentstyle\b/) {
                $format = 'latex';
                $score++;
            }

            if (/^\\title\{/i or
                /^\s*\\begin\s*\{abstract\}\b/i or
                /^\s*\\section\s*\{INTRODUCTION\}\b/i or
                /^\s*\\begin\s*\{(chapthebibliography|thebibliography|references)\}/i) {
                $score++;
            } elsif (/^\s*\\shorttitle\s*\{(.*)\}/i) {
                $title = $1;
                $score++;
            } elsif (/^\s*\\newcommand\s*\{\\([^\}]+)\}.*?\{\\bibitem\b/i) {
                $bibitem = $1 unless ($bibitem);
            } elsif (/^\s*\\def\{?\\(.+?)\{\\bibitem\b/i) {
                $bibitem = $1 unless ($bibitem);
            } elsif (/^s*\\input\{\s*(\S*?)\s*\}/ || /^s*\\input\s+(\S*?)/) {
                # if the file is included by another one, most likely
                # is not the main tex/latex source
                $notmain{$1}++;
            }
        }

        close($fh);

        push(@candidates, {
            file     => $infile,
            basename => $basename,
            score    => $score,
            bibitem  => $bibitem,
            title    => $title,
            fmt      => $format,
            ignore   => $ignore
        });
    }

    # now pull it all together and figure out which files are and which
    # are not main articles

    foreach my $c (@candidates) {
        if ($notmain{$c->{file}}) {
            $c->{score} = -2;
        } elsif ($notmain{$c->{basename}}) {
            $c->{score} = -1;
        }
    }

    # sort by score (highest first), and provide default values
    # to some of the missing fields

    local($a, $b);

    @candidates = sort { $b->{score} <=> $a->{score} } @candidates;
    my ($def_bibitem, $def_title);

    foreach my $c (@candidates) {
        $def_bibitem ||= $c->{bibitem};
        $def_title   ||= $c->{title};
    }

    foreach my $c (@candidates) {
        $c->{bibitem} ||= $def_bibitem;
        $c->{title}   ||= $def_title;
    }

    if ($debug > 1) {
        use Data::Dumper;
        warn "main paper candidates:\n",
        Data::Dumper->Dump([ @candidates ]);
    }

    return @candidates;
}

sub cleanup_tmp {
    my $tmp = shift;
    my $force = shift;

    if ($keep_tmp and not $force) {
        warn "directory $tmp not removed\n";
        return 0;
    }

    system("/bin/chmod -R 755 $tmp") and
        warn "cannot chmod 755 $tmp\n";
    system("/bin/rm -rf $tmp") and
        warn "cannot rm -rf $tmp\n";
}

# runs an external command enforcing a timeout
# we used to do this with /usr/bin/timeout but that command
# disappeared in CentOS 4.x
sub SystemTimeout {
    use POSIX ":sys_wait_h";
    my $timeout = shift;
    my $command = join(' ', @_);
    my $ret = 256;
    my $timestart = time;

    my $child_pid = fork();

    if (!defined($child_pid)) {
        die "$script: Fork Error";
    } elsif ($child_pid) {
        # This is the parent.  Wait and kill if necessary.
        # (Eval/alarm structure taken from perlipc man page.)
        eval {
            local $SIG{ALRM} = sub { die "$script: timeout!" };
            alarm $timeout;
            waitpid($child_pid,0);
            $ret = ($? >> 8);
            alarm 0;
        };

        if ($@ and $@ =~ /timeout/) {
            # Kill the child and all in its process group.
            warn "$script: command '$command' timed out after $timeout seconds\n";
            # these signals are: (TERM HUP KILL)
            for my $signal (qw(15 1 9)) {
                last if kill($signal, -$child_pid);
                sleep 2;
                waitpid($child_pid,WNOHANG);
                warn "$script: warning: kill of $child_pid (via $signal) failed...\n";
            }
        } else {
            warn "$script: command '$command' executed in ", time - $timestart, " seconds\n"
                if ($debug);
            $ret = 0;
        }
    } else {
        # This is the child.  Give myself a new process group,
        # then run the command.
        warn "$script: executing: $command\n"
            if ($debug);
        setpgrp($$);
        exec("$command </dev/null >/dev/null 2>/dev/null");
        warn "$script: exec '$command' failed: $!\n";
    }

    return $ret;
}

# creates a regexp that can be used to fix preprint
# categories that have been collapsed.  E.g.
#     astroph/1234567   ->   astro-ph/1234567
# we have to do this because pdftotext tries to be
# overly clever and merges things together at times
sub GetSplitEprints {
    use ADS::OAI::ArXiv;
    my @ecats = &ADS::OAI::ArXiv::Sets;
    my %hash;

    foreach my $cat (@ecats) {
        my $re = $cat;
        $re =~ s/\-//;
        $hash{$re} = $cat;
    }

    return %hash;
}
