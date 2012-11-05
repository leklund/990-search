#!/usr/bin/env perl

use strict;
use warnings;
use DateTime;
use Cwd qw(abs_path);

# bulk resource has data back through 2002 and their rsync is SLOOOOOOOOW.
# So instead generate links to every meta data and raw file per month.
# The format is:
# 2012_09_T/manifest.2012_09_T.txt
# 2012_09_T/irs.2012_09_T.dat.txt
# 2012_09_EO/manifest.2012_09_T.txt
# 2012_09_EO/irs.2012_09_T.dat.txt
# 2012_09_PF/manifest.2012_09_T.txt
# 2012_09_PF/irs.2012_09_T.dat.txt
#
# There is also a file called "htaccess.2012_09_T.txt" that has some nice human
# readable descriptions.

my $dest= abs_path($0);
$dest =~ s/scripts\/get_meta\.pl$/manifests\//ogi;
unless (-d $dest) {
  mkdir $dest or die "Unable to mkdir $dest: $!\n";
}

my $start = DateTime->new(
  year  =>  2002,
  month =>  1,
  day   =>  1
);
my $end = DateTime->now;
my $dur = $start - $end;

my $totmo = ($dur->years * 12) + $dur->months;

foreach my $mo (0..$totmo) {
  $mo = 1 if $mo > 1;
  $start->add(months => $mo);

  my $ym = $start->strftime('%Y_%m');
  foreach my $type (qw/T EO PF/) {
    next if $type eq "T" && $start->year < 2009;
    my $name = $ym . "_" . $type;
    my $final_dest = "$dest$name";
    unless (-d $final_dest) {
      mkdir $final_dest or die "Unable to mkdir $final_dest: $!\n";
    }

    my $cmd = qq{rsync -rtzq --exclude="htaccess*" --chmod=u+w bulk.resource.org::bulk/irs.gov/eo/$name/*$name*.txt $final_dest};

    my $out = system($cmd);
    if ($out != 0) {
      print "command failed $? \n  $cmd\n";
    } else {
      print POSIX::strftime('%H:%M:%S', localtime) . " - $name - success!\n";
    }
  }
}

