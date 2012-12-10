#!/usr/bin/env perl

use strict;
use warnings;
use File::Find;
use DateTime;
use DBIx::DataStore;
use Digest::MD5::File qw( file_md5_hex );
use Cwd qw(abs_path);
use File::Basename;
use threads;
use Thread::Queue;

use Data::Dumper;

my $THREADS = 5;
# threading failed for the triggered inserts. stupid deadlocks.
my $THREADSUCK = 1;
# work queue
my $qbert = Thread::Queue->new();
my $qbert20 = Thread::Queue->new();


# get all irs files in the manifests directory
# check ot see if they have been imported
# if not, import
#
# this fails if files were modified after rsync and import
# but we'll save a hash of the imported file just in case
my $manifests = abs_path($0);
$manifests =~ s/scripts\/import\.pl/manifests\//ogi;
chdir $manifests;
#my @files = glob('*/irs.2002*.txt');
my @files = glob('*/irs*.txt');


foreach (@files) {
  my $full_path = $_;
  my $filename = my $basename = basename($full_path);
  my $md5 = file_md5_hex($full_path);
  $basename =~ s/irs\.(.+?)\..+$/$1/o;
  my $db = get_db_handle();
  my $res = $db->do(q{ select id, imported, hash from files where name = ?}, $filename);
  my $q = {file => $filename, md5 => $md5, basename => $basename, full_path => $full_path};
  if ($res && $res->count > 0 && $res->next) {
    print "filename: $filename id: $res->{id} hash: $res->{hash} \n";
    $qbert->enqueue($q) if !$res->{'imported'};
    # check the file hash
    if ($md5 ne $res->{'hash'}) {
      # hash mismatch
      print "Hash mismatch for $filename\n";
    }
  } else {
    $qbert->enqueue($q);
  }
}

#spawn worker threads
threads->create("importer") for(1..$THREADS);
# faux signals
$qbert->enqueue("EXIT") for(1..$THREADS);

while(threads->list(threads::running)){
  sleep 10;
}

print "file imports complete. fire triggers at will\n";
#part one. done.
threads->create("finisher") for (1..$THREADSUCK);
$qbert20->enqueue("EXIT") for(1..$THREADSUCK);

while(threads->list(threads::running)){
  sleep 10;
}

sub importer {
  my $db = get_db_handle();
  while(my $work=$qbert->dequeue()) {
    last if $work eq 'EXIT';
    $db->begin;
    my $ins = {
      name => $work->{'file'},
      base_name => $work->{'basename'},
      hash => $work->{'md5'}
    };
    my $res = $db->do(q{ insert into files ??? returning id}, $ins);
    my $file_id = undef;
    if ($res && $res->next) { $file_id = $res->{id}; }
    $db->commit;
    my $tmpfile = "$work->{'file'}.tempfile";
    my $out = system(q|awk  'BEGIN { FS =",";OFS=","}; {print "| . $file_id . q|",$1,$2,$3,$4,$5,$6,$7,$8,$9}' | . qq{ $work->{full_path} > $tmpfile});
    #my $out = system(qq{cut -d , -f1-9 $work->{full_path} > $tmpfile});
    if ($out == 0) {
      #remove pesky carriage returns
      `sed -i '' -e 's///g' $tmpfile`;
      # remove backslashes preceding commas
      `sed -i '' -e 's/\\\\\\,/,/g' $tmpfile`;
      my $cmd = qq{PGPASS=b4k3rSTReeT psql -U sherlock -h 127.0.0.1 ninenine -c "\\copy npo.irs_raw (file_id,ein,filing_period,taxpayer_name,state,zip,return_type,subsection_code,total_assets,scan_date) from $tmpfile with delimiter ','"};
      my $sqlout =  `$cmd 2>&1`;
      print "import finished $work->{file}\n";
      if ($sqlout =~ /error/i) {
        print "There seems to have been an error with $work->{file}\n$sqlout\n";
        print "Fix the file and try again\n";
        $db->begin;
        $res = $db->do(q{ delete from files where id = ? }, $file_id);
        $db->commit;
        next;
      } else {
        $qbert20->enqueue($file_id);
      }
      unlink $tmpfile;
    } else {
      #FAILURE to awk the file
    }
  }
  threads->detach;
}

sub finisher {
  my $db = get_db_handle();
  while(my $file_id=$qbert20->dequeue()) {
    print "next file $file_id ...  \n";
    last if $file_id eq 'EXIT';
    $db->begin;
    my $res = $db->do(q{ update files set imported = 't' where id = ? }, $file_id);
    $db->commit if $db->in_transaction;
  }
  threads->detach;
}


sub get_db_handle {
  my $conf = {
    cache_connections => 0,
    primary => {
      driver => 'Pg',
      database => 'ninenine',
      host => '127.0.0.1',
      user => 'sherlock',
      pass => 'b4k3rSTReeT',
      schemas => ['npo','public'],
      dbd_opts => {
        AutoCommit => 0
      }
    },
  };
  my $dbh = DBIx::DataStore->new({config => $conf});
}
