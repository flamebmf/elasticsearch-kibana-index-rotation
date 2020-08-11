#!/usr/bin/perl -w 
############################## ELK DB rotating script ##############
# Version : 1.1
# Date : jun 03 2020
# Author  : Akulich Dmitry
# 
#################################################################
#
# Help :  -h
#
use Getopt::Long;
use REST::Client;
use DateTime;
my $elk_uri="http://localhost:9200/";
my $list_index="_cat/indices?v";
my $t_zone="Europe/Moscow";
my $o_help=	undef; 		# wan't some help ?
my $o_index;		# index to rotate
my $o_days=	undef;# Days period
my $logfile='/tmp/ELK-rotation.dbg';
my @result;
my @index_list;
my $client= REST::Client->new();
my $dt1=DateTime->from_epoch(epoch=> time(),time_zone => $t_zone,);
my $dt2=DateTime->from_epoch(epoch=> time(),time_zone => $t_zone,);
my %OPTION = (
    "index" => undef,
    "days" => undef,
    "debug" => undef,
    );
#subs
sub check_options {
    Getopt::Long::Configure ("bundling");
	GetOptions(
        "I|index=s"         => \$OPTION{'index'},
        "D|days=s"             => \$OPTION{'days'},
        "debug=s"	=> \$OPTION{'debug'},
        'h'     => \$o_help,    	'help'        	=> \$o_help,
        );
    if (defined ($o_help) ) { help(); exit };
    if (defined ($OPTION{'debug'}) ) {
    open(FILE,"+>>","$logfile") or die "can't open debug file for writing\n";
    print FILE "$dt1\t parcing options  options: I=$OPTION{'index'}, D=$OPTION{'days'}, debug=$OPTION{'debug'}\n";
    close (FILE);
    };
    
}
sub print_usage {
    print "Usage: $0 -I <index name without datetime stamp> -D <number of days  to keep> [-2] | --debug <yes show debug info>  \n";
    if (defined ($OPTION{'debug'}) ) {
    open(FILE,"+>>","$logfile") or die "can't open debug file for writing\n";
    print FILE "$dt1\t help requested\n";
    close (FILE);
    };
}
sub help {
   print "\nELK Index rotation \n";
   print "(c)2020 Akulich Dmitry\n\n";
   print_usage();
   print <<EOT;
-h, --help
   print this help message
-I, --index=elastiflow
   index name to rotate
-D, --days=Days to keep

EOT
if (defined ($OPTION{'debug'}) ) {
    open(FILE,"+>>","$logfile") or die "can't open debug file for writing\n";
    print FILE "$dt1\t help requested\n";
    close (FILE);
    };
}

#main ----------------------------------------
$num_args = $#ARGV + 1;

if ($num_args <1) {
    print "No arguments !\n";
    print_usage();
    exit(1);
}

check_options();

if (defined ($OPTION{'debug'}) ) {
    open(FILE,"+>>","$logfile") or die "can't open debug file for writing\n";
    print FILE "$dt1\t main code\t index filter=$OPTION{'index'}\n";
    close (FILE);
    };

$client->addHeader ('kbn-xsrf','true');
$client->addHeader ('Content-Type','application/json');
$client->GET("$elk_uri"."$list_index");
$response=$client->responseContent();
@result=split(/\n/,$response);

foreach $line(@result) {
    $line=~ s/\t+/ /;
    $line=~ s/\s+/ /;
    $line=~ s/\t/ /;
    (my $a,my $b,my $index_,$a,$b,$a,$b,my $size,my $pri_size)=split(/\s+/,$line);

    if ($index_=~ m"$OPTION{'index'}") {
	
	$index_=~ /($OPTION{'index'})\-(\d+).(\d+).(\d+)/;
	$dt2->set( year=>$2, month=>$3,day=>$4,);
	$dur=$dt1->delta_days($dt2)->delta_days();

	if (defined ($OPTION{'debug'}) ) {
	    open(FILE,"+>>","$logfile") or die "can't open debug file for writing\n";
	    print FILE "index was found =$index_ dt2=$dt2 delta_days=$dur\n";
	    print "found index=$index_ delta=$dur day(s)\n";
	    close (FILE);
	    
	    };

	if ($dur>$OPTION{'days'}) {
	    push  @index_list, $index_;
	    $client->DELETE("$elk_uri"."$index_"."\?pretty");
	    
		if (defined ($OPTION{'debug'}) ) {
		open(FILE,"+>>","$logfile") or die "can't open debug file for writing\n";
		print FILE "$dt1\t index have been deleted $index_  delta_days=$dur\n";
		close (FILE);
		};

	    };

	};
};
print "Deleted indexes\:\n @index_list\n";
exit(0);
