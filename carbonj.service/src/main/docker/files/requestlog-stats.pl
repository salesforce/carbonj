#!/usr/bin/env perl
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


####  graphite-stats.pl
####  Parse Nginx logs and build some stata
####
#### Gary Rule
#### grule@demandware.com
#### Christian Bayer
#### christian.bayer@salesforce.com
#### 11/16/15
#### 11/19/15
#### 12/07/17
#### 02/01/18

use warnings;
use strict;
use Getopt::Long;
use Term::ANSIColor;
use IO::Socket::INET;
Getopt::Long::Configure ("ignorecase","bundling");
use Data::Dumper;
use URI::Encode qw (uri_decode);
use Sys::Hostname;
use POSIX qw(strftime);

### Vars
my $live                = defined($ENV{'DW_DEBUG'}) ? 0 : 1;
my $useTCP      	    = defined($ENV{'DW_USE_TCP'}) ? 1 : 0;
my $processRequestLog	= 1;
my $processError	    = 0;
my $processSystem	    = 1;
my $startTime		    = time;
my $pageSize            = 4096;
my $carbonServer	    = $ENV{'DW_GRAPHITE_HOST'};
my $carbonPort		    = 2003;
my $podID		        = $ENV{'dw.podId'};
my $groupId		        = $ENV{'DW_GROUP_ID'};
my $hostID		        = hostname;
my $serviceName         = $ENV{'SVC_PROP_APP_NAME'};
my $serviceNameVersion  = $ENV{'DW_SVC_VERSION'};
my $prefix              = $ENV{'DW_PREFIX'};
my $date                = strftime "%Y%m%d", localtime;
my $ngxAccLog;
my $ngxErrLog;
my $ngxAccCheckFile	    = defined($ENV{'DW_DEBUG'}) ? "/dev/null" : "/tmp/request_log.check";
my $ngxErrCheckFile	    = defined($ENV{'DW_DEBUG'}) ? "/dev/null" : "/tmp/error_log.check";
my $verbose		        = 1;
my $accessPos 		    = 0; 				# Set File Position Market to 0
my $errorPos		    = 0; 				# Set File Position Market to 0
my $grabCrit 		    = '.*'; 			# We need all lines from the respective logs
my @accProcessCriteria	= ('render','metrics');
my @errProcessCriteria	= ('Connection timed out','Connection reset by peer');
my @processStats	    = ('nginx', 'uwsgi','/usr/bin/gunicorn');
my @accLines;
my @errLines;
if (defined $ENV{'DW_ACCESS_LOG_FILE'}) {
    $ngxAccLog 		    = $ENV{'DW_ACCESS_LOG_FILE'};
} elsif ($prefix eq 'mellon') {
    $ngxAccLog 		    = "/var/log/apache.log";
} elsif ($prefix eq 'nginx') {
    $ngxAccLog 		    = "/var/log/nginx/access.log";
} elsif ($prefix eq 'jetty') {
    $ngxAccLog          = "/app/log/request-$hostID-$date";
}

if (defined $ENV{'DW_ERROR_LOG_FILE'}) {
    $ngxErrLog 		= $ENV{'DW_ERROR_LOG_FILE'};
} elsif ($prefix eq 'mellon') {
    $ngxErrLog 		= "/var/log/error.log";
} elsif ($prefix eq 'nginx') {
    $ngxErrLog 		= "/var/log/nginx/error.log";
}

# post-process host-id and service name
$hostID		        =~ s/\./_/g;
$serviceNameVersion =~ s/\./_/g;

### PROTO
sub sendStats($$$$$$$);
sub processRequestLogData($@);
sub processErrData($@);
sub systemMetrics();
sub grabLines($$$);
sub getCheck($);
sub writeCheck($$);
sub writeCarbon($$$);


###################################
### PROCESS PROCESS METRICS :)
if ($processSystem) {
	&systemMetrics;
}
### ACCESS LOG
if ($processRequestLog) {
	if (-e $ngxAccCheckFile) {
		$accessPos			= &getCheck($ngxAccCheckFile);			# Read the check file and get the byte count
		($accessPos,@accLines) 		= &grabLines($ngxAccLog,$accessPos,$grabCrit);	# Use $grabCrit to capture matching lines in $ngxAccLog. Return byte count and array of lines
	} else {
		print color("red"),"Didn't find $ngxAccCheckFile...creating it\n",color("reset") if $verbose;
		($accessPos,@accLines) 		= &grabLines($ngxAccLog,'0',$grabCrit);		# Use $grabCrit to capture matching lines in $ngxAccLog. Return byte count and array of lines
	}
}
### ERROR LOG
if ($processError) {
	if (-e $ngxErrCheckFile) {
		$errorPos			= &getCheck($ngxErrCheckFile);			# Read the check file and get the byte count
		($errorPos,@errLines) 		= &grabLines($ngxErrLog,$errorPos,$grabCrit);	# Use $grabCrit to capture matching lines in $ngxAccLog. Return byte count and array of lines
	} else {
		print color("red"),"Didn't find $ngxErrCheckFile...creating it\n",color("reset") if $verbose;
		($errorPos,@errLines) 	= &grabLines($ngxErrLog,'0',$grabCrit);		# Use $grabCrit to capture matching lines in $ngxAccLog. Return byte count and array of lines
	}
}
### WRITE CHECKS
if ($processRequestLog) {
	&writeCheck($accessPos,$ngxAccCheckFile);					# Write the new byte count, returned by grabLines, to the check file
}
if ($processError) {
	&writeCheck($errorPos,$ngxErrCheckFile);					# Write the new byte count, returned by grabLines, to the check file
}

### PROCESS ACCESS DATA
if ($processRequestLog) {
    my $dbData; # reference to hash
    my $statusCodes; # reference to hash
    my $nameSpace = "$podID.$groupId.$serviceName.$hostID.$serviceNameVersion.$prefix.response";
    foreach my $criteria (@accProcessCriteria) {
        (my $count,
            my $renderTime, my $renderMax, my $p95, my $p99,
            my $bytesTotal, my $bytesMax, my $bytesP95, my $bytesP99,
            my $lenTotal, my $lenMax, my $lenP95, my $lenP99,
            $statusCodes, $dbData) = &processRequestLogData($criteria, @accLines);

        # bytes are reported by all
        sendStats($count, $bytesTotal, $bytesMax, $bytesP95, $bytesP99, "$nameSpace.$criteria", "size");
        # mellon doesn't report render time
        if (!($prefix eq 'mellon')) {
            sendStats($count, $renderTime, $renderMax, $p95, $p99, "$nameSpace.$criteria", "time");
        }
        # only nginx reports request length
        if ($prefix eq 'nginx') {
            sendStats($count, $lenTotal, $lenMax, $lenP95, $lenP99, "$nameSpace.$criteria", "len");
        }
        while (my ($k, $v) = each %$statusCodes) {
            writeCarbon("$nameSpace.$criteria.status.$k", "$v", $startTime);
        }
        while (my ($k, $v) = each %$dbData) {
            writeCarbon("$nameSpace.$criteria.$k", "$v", $startTime);
        }
    }
}

### PROCESS ERROR DATA
if ($processError) {
	my %dbErrors;
	foreach my $criteria (@errProcessCriteria) {
		my ($count,@urls) = &processErrData($criteria,@errLines);

		if ($criteria =~ /reset/) {
			writeCarbon("err.connreset",$count,$startTime);
		}
		if ($criteria =~ /timed/) {
			writeCarbon("err.conntimeout",$count,$startTime);
			foreach my $url (@urls) {
				if ($url) {
					my @urlParts = split(/\?/,$url);
					$url = $urlParts[0];
					$url =~ s/http:\/\/um.demandwarecloud.net\/grafana:3000//;
					$url =~ s/https:\/\/um.demandwarecloud.net\/grafana:3000//;
					$url =~ s/http:\/\/um.demandwarecloud.net\/grafana//;
					$url =~ s/https:\/\/um.demandwarecloud.net\/grafana//;
					$url =~ s/http:\/\/um.demandwarecloud.net\/grafana4//;
					$url =~ s/https:\/\/um.demandwarecloud.net\/grafana4//;
					$url =~ s/dashboard\/db\///;
					$url =~ tr/"//d;
					$dbErrors{"err.detail.conntimeout.$url"}++;
				}
			}
		}
	}
	while (my ($k,$v) = each %dbErrors) {
		writeCarbon("$k","$v",$startTime);
	}
}


###################################

### SUBS
sub sendStats($$$$$$$) {
    my ($count, $total, $max, $p95, $p99, $criteria, $name)	= @_;
    if ($total != 0) {
        my $mean;
        $mean = $total / $count;
        $mean = sprintf "%.2f", $mean;
        writeCarbon("$criteria.$name.mean",$mean,$startTime);
    }
    writeCarbon("$criteria.$name.count",$count,$startTime);
    writeCarbon("$criteria.$name.max",$max,$startTime);
    writeCarbon("$criteria.$name.p95",$p95,$startTime);
    writeCarbon("$criteria.$name.p99",$p99,$startTime);
}

sub grabLines($$$) {
	my ($file,$pos,$crit) = @_;
	my @lines;

	open my $file_fh, '<', $file || die("Can't open $file: $!");

	my $fileSize = -s $file;
	if ($fileSize < $pos) {
		print "New File, rewinding\n" if $verbose;
		seek($file_fh,0,0);
	} else {
		seek($file_fh,$pos,0);
	}

	while(my $buffer = <$file_fh>) {
		if ($buffer =~ /$crit/) {
			push @lines, $buffer;
		}
	}

	my $curPos = tell($file_fh);
	close($file_fh);
	return($curPos,@lines);
}

sub processRequestLogData($@) {
	my ($crit,@data)	= @_;
	my $count 		    = 0;
	my $renderTime 		= 0;
	my $renderMax		= 0;
	my $p95 			= 0;
	my $p99 			= 0;
	my %dbData;
	my @rTimes;
	my $bytesTotal      = 0;
	my $bytesP95        = 0;
	my $bytesP99        = 0;
	my $bytesMax        = 0;
	my @bytesTotalArr;
	my $lenTotal        = 0;
	my $lenP95          = 0;
	my $lenP99          = 0;
	my $lenMax          = 0;
	my @lenTotalArr;
	my %statusCodes;

	foreach my $line (@data) {
        next unless $line =~ /$crit/;
        $count++;
		chomp $line;
		$line =~ s/\s+/ /go;
        my $rtime;
        my $len;
		my $clientAddress;
		my $vhost;
		my $rfc1413;
		my $username;
		my $localTime;
		my $httpRequest;
		my $status;
		my $bytes;
		my $referer;
		my $clientSoftware;
		my $forwarededFor;
		my $connection_requests;
		my $upstream_addr;
		my $upstream_cache_status;
		my $request_body;

        if ($prefix eq 'jetty') {
            my $first; #regex matches an emtpy string for each line in the file for the first element here. not sure why.
            ($first,
                $clientAddress,
                $rfc1413,
                $username,
                $localTime,
                $httpRequest,
                $status,
                $bytes,
                $rtime) = split(
                    /^(\S+) (\S+) (\S+) \[(.+)\] \"(.+)\" (\S+) (\S+) (\S+)/o, $line);
        }
        elsif ($prefix eq 'nginx') {
            #nginx:
            #		log_format post_data '$remote_addr - $remote_user [$time_local] "$request" '
            #                '$status $body_bytes_sent "$http_referer" '
            #                '"$http_user_agent"
            #		 		 "$http_x_forwarded_for" '
            #                'Len: $request_length Rtime: $request_time Reqs: $connection_requests '
            #                'UPSTR: $upstream_addr Cache: $upstream_cache_status '
            #                'POST-DATA: $request_body'
            my $first;
			($first, #regex matches an emtpy string for each line in the file for the first element here. not sure why.
                $clientAddress,
				$rfc1413,
				$username,
				$localTime,
				$httpRequest,
				$status,
				$bytes,
				$referer,
				$clientSoftware,
				$forwarededFor,
				$len,
				$rtime,
                $connection_requests,
				$upstream_addr,
				$upstream_cache_status,
                $request_body) = split(
                /^(\S+) (\S+) (\S+) \[(.+)\] \"(.+)\" (\S+) (\S+) \"(.*)\" \"(.*)\" \"(.*)\" Len: (\S+) Rtime: (\S+) Reqs: (\S+) UPSTR: (\S+) Cache: (\S+) POST-DATA: (\S+)/o, $line);
        }
        elsif ($prefix eq 'mellon') {
            my $first;
            my $second;
			($first, #regex matches an emtpy string for each line in the file for the first element here. not sure why.
                $second,
                $vhost,
                $clientAddress,
				$rfc1413,
				$username,
				$localTime,
				$httpRequest,
				$status,
				$bytes,
				$referer,
				$clientSoftware) = split(
                /^((\S+:\S+) )?(\S+) (\S+) (\S+) \[(.+)\] \"(.+)\" (\d+) (\d+)( \"(.*)\" \"(.*)\")?/o, $line);
        }

        if (!$live) {
            print $line;
            print "bytes: $bytes\n";
            if (defined $rtime) {
                print "rtime: $rtime\n";
            }
            if (defined $len) {
                print "len: $len\n";
            }
            print "status: $status\n";
        }

        if ($prefix eq 'nginx') {
            my @urlParts = split(/dashboard\/db\//, $referer);
            if (scalar @urlParts > 1) {
                my $url = $urlParts[1];
                $url =~ s/\?.*//;
                $url =~ s/\.//;
                $url =~ tr/"//d;
                $dbData{"dashboard.count.$url"}++;

                if (!$live) {
                    print "referer: $referer\n";

                    print "urlparts: ";
                    foreach (@urlParts) {
                        print "$_\r\n";
                    }
                }
            }
        }

        if (defined $rtime) {
            $renderTime = $renderTime + $rtime;
            push @rTimes, $rtime;
            if ($rtime > $renderMax) {
                $renderMax = $rtime;
            }
        }

        $bytesTotal = $bytesTotal + $bytes;
        push @bytesTotalArr, $bytes;
        if ($bytes > $bytesMax) {
            $bytesMax = $bytes;
        }

        if (defined $len) {
            $lenTotal = $lenTotal + $len;
            push @lenTotalArr, $len;
            if ($len > $lenMax) {
                $lenMax = $len;
            }
        }
        $statusCodes{"$status"}++;
    }

    if (scalar @rTimes > 0) {
        my @rTimesSorted = sort { $a <=> $b } @rTimes;
        $p95 = $rTimesSorted[int(@rTimesSorted * 95 / 100)];
        $p99 = $rTimesSorted[int(@rTimesSorted * 99 / 100)];
    }

    if (scalar @bytesTotalArr > 0) {
        my @bytesTotalArrSorted = sort {$a <=> $b} @bytesTotalArr;
        $bytesP95 = $bytesTotalArrSorted[int(@bytesTotalArrSorted * 95 / 100)];
        $bytesP99 = $bytesTotalArrSorted[int(@bytesTotalArrSorted * 99 / 100)];
    }

    if (scalar @lenTotalArr > 0) {
        my @lenTotalArrSorted = sort {$a <=> $b} @lenTotalArr;
        $lenP95 = $lenTotalArrSorted[int(@lenTotalArrSorted * 95 / 100)];
        $lenP99 = $lenTotalArrSorted[int(@lenTotalArrSorted * 99 / 100)];
    }

	return ($count,
            $renderTime,$renderMax,$p95,$p99,
            $bytesTotal,$bytesMax,$bytesP95,$bytesP99,
            $lenTotal,$lenMax,$lenP95,$lenP99,
            \%statusCodes,
            \%dbData);
}

sub processErrData($@) {
	my ($crit,@data) 	= @_;
	my $count		= 0;
	my @urls;
	foreach my $line (@data) {
		next unless $line =~ /$crit/;
		$count++;
		my $url;
		my @lineParts	= split(/\s+/,$line);
		if ($crit =~ /reset/) {
			$url		= $lineParts[24];
		}
		if ($crit =~ /timed/) {
			$url		= $lineParts[31];
		}
		push @urls, $url;
	}
	return ($count,@urls);
}

sub systemMetrics() {
	### Individual Process Metrics First
	foreach my $process (@processStats) {
		open my $processList_h, "ps auxwww |grep $process |grep -v grep|" || die("Can't open ps: $!\n");
		my $count 		= 0;
		my $cpuCount 		= 0;
		my $memSizeCount 	= 0;
		my $memResCount 	= 0;
		my $cpuPercentCount	= 0;
		while(my $buffer = <$processList_h>) {
			$count++;
			my @parts 	= split(/\s+/,$buffer);
			my $pid 	= $parts[1];

			### MEMORY
			open my $processInfo_h, '<', "/proc/$pid/statm"  || die("Can't open /proc/$pid/stat: $!\n");
			while(my $buffer = <$processInfo_h>) {
				my @parts 	= split(/\s+/,$buffer);
				my $size 	= $parts[0];
				my $resident 	= $parts[1];
				$memSizeCount 	= $memSizeCount + $size;
				$memResCount 	= $memResCount + $resident;
			}
			close($processInfo_h);

			### CPU
			my $cpuPercent 	 = `ps -p $pid -o %cpu --no-headers`;
			$cpuPercentCount = $cpuPercentCount + $cpuPercent;
		}

	        ### if the process is running
	        if ( $count > 0 ) {

	            ### Multiply by page size
	            $memSizeCount = $memSizeCount * $pageSize;
	            $memResCount = $memResCount * $pageSize;

                $process =~ s/\//_/g;
                my $nameSpace = "$podID.$groupId.$serviceName.$hostID.$serviceNameVersion.proc.$process";
	            ### Write to carbon
	            writeCarbon("$nameSpace.proc.count", $count, $startTime);
	            writeCarbon("$nameSpace.cpu.cpuPercent", $cpuPercentCount, $startTime);
	            writeCarbon("$nameSpace.mem.memSize", $memSizeCount, $startTime);
	            writeCarbon("$nameSpace.mem.memRez", $memResCount, $startTime);
	        }
    }

    #### DISK
    #my $metricsDiskUsage  = `df -h --output=pcent /opt/g1  |grep -v Use |tr -d \%`;
    #chomp($metricsDiskUsage);
    #writeCarbon("disk.usagePercent",$metricsDiskUsage,$startTime);
}

sub writeCarbon($$$) {
    my ($id,$val,$stamp) = @_;
	if ($live) {
		print "Sending: $id $val $stamp\n" if $verbose;
        my $socket;
        if ($useTCP) {
            $socket = IO::Socket::INET->new(
                PeerAddr => $carbonServer,
                PeerPort => $carbonPort,
                Proto    => 'tcp');
            die "Unable to connect: $!\n" unless ($socket->connected);
        } else {
            $socket = IO::Socket::INET->new(
                PeerAddr => $carbonServer,
                PeerPort => $carbonPort,
                Proto    => 'udp') or die "ERROR in Socket Creation : $!\n";
        }
        $socket->send("$id $val $stamp\n");
        $socket->shutdown(2);
        $socket->close();
	} else {
		print "NOT Sending: $id $val $stamp\n" if $verbose;
	}
}
sub getCheck($) {
	my ($file) = @_;
	my $checkLine;
	open my $file_fh, '<', $file ||die ("Can't open $file for reading: $!");
	$checkLine = <$file_fh>;
	close($file_fh);
	return $checkLine;
}
sub writeCheck($$) {
	my ($checkLine,$file) = @_;
	open my $file_fh, '>', $file || die ("Can't open $file for writing: $!");
	print $file_fh $checkLine;
	close($file);
}
