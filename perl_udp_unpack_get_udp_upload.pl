#!/usr/bin/perl -w
use warnings;
use Date::Format;
use strict;
use DBI;
use utf8; 
use Encode qw(encode decode);
use IO::Socket;
use Geo::Distance::XS;
use Config::Tiny;

# Create a config
my $Config = Config::Tiny->new;
my ($aktion, $parameter) = @ARGV;

if (defined $aktion && $aktion eq "genconfigexample") {
	&genconfigexample();
	exit;
}

$Config = Config::Tiny->read( '/etc/fanet.conf', 'utf8' ) or $Config = Config::Tiny->read( 'fanet.conf', 'utf8' ) or die "Bitte Konfig-Datei erstellen"; # Neither ':' nor '<:' prefix!



my $geo = Geo::Distance->new;

my $dbh = DBI->connect('dbi:MariaDB:dbname='.$Config->{mysql}->{db}.';host='.$Config->{mysql}->{host},$Config->{mysql}->{user},$Config->{mysql}->{pwd},{AutoCommit=>1,RaiseError=>0,mariadb_auto_reconnect=>1,PrintError=>1});
my $dbh2 = DBI->connect('dbi:MariaDB:dbname='.$Config->{mysql2}->{db}.';host='.$Config->{mysql2}->{host},$Config->{mysql2}->{user},$Config->{mysql2}->{pwd},{AutoCommit=>1,RaiseError=>0,mariadb_auto_reconnect=>1,PrintError=>1});
	
my $received_by = "";


if      (defined $aktion && $aktion eq "GETfromUDP") { # Port 10110 - like nmea PSRFI and #FNF
	&GETfromUDP();
} elsif (defined $aktion && $aktion eq "GETfromUDP2") { # Port 12390 - RAW
	&GETfromUDP2(); 
} elsif (defined $aktion && $aktion eq "SENDtoUDP") {
	&SENDtoUDP($parameter);
} elsif (defined $aktion && $aktion eq "GETfromSQL_db") {
	&GETfromSQL("db");
} elsif (defined $aktion && $aktion eq "GETfromSQL_send") {
	&GETfromSQL("send");
} elsif (defined $aktion && $aktion eq "process_line") {
	&process_line($parameter);
} elsif (defined $aktion && $aktion eq "remove_and_create_tables") {
	&remove_and_create_tables();
} elsif (defined $aktion && $aktion eq "GXAirComFNT") {
	&GXAirComFNT();
} elsif (defined $aktion && $aktion eq "GETfromFile") {
	&GETfromFile();
}  else {
	print " HELP
GETfromUDP              - listen to udp Port 10110 - like nmea
GETfromUDP2             - listen to udp Port 10110 - RAW - (mode bridge SOFTRF)
SENDtoUDP parameter     - send something to port Port 12390 (mode bridge SOFTRF)
GETfromSQL_db           - select from db and process
GETfromSQL_send         - select from db and send
process_line  parameter - process the line e.g. 'PSRFI,1592069109,4401240126f3f94541b105f628330f0000000000000000000000000000000000,00'
remove_and_create_tables- remove and create tables- init SQL
GXAirComFNT             - send #FNT to PORT 10110
genconfigexample        - generate example fanet.conf file -have to be edited later 
GETfromFile             - read file and process it again
";
}

sub SENDtoUDP {
  my( $socket, $data);
  $socket = new IO::Socket::INET (PeerAddr   => $Config->{udp}->{host}.':12389', Proto        => 'udp') or die "ERROR in Socket Creation : $!\n ";
#send operation
my( @hex ) = unpack( '(A2)*', $_[0] );
my( $newencoded ) = pack( '(H2)*', @hex );
$socket->send($newencoded);
}

sub genconfigexample {
	my $Config2 = Config::Tiny->new;
	$Config2->{mysql} = { host => 'localhost' }; 
	$Config2->{mysql}->{db}  = 'SOFTRF' ;
	$Config2->{mysql}->{user} = 'SOFTRF' ;
	$Config2->{mysql}->{pwd} = 'password' ;
	
	$Config2->{mysql2} = { host => 'localhost' }; 
	$Config2->{mysql2}->{db}  = 'SOFTRF' ;
	$Config2->{mysql2}->{user} = 'SOFTRF' ;
	$Config2->{mysql2}->{pwd} = 'password' ;
	$Config2->{mysql2}->{send_data} = 'no' ;
	
	$Config2->{nmea} = { log => 'yes' }; 
	$Config2->{nmea}->{path}  = '/tmp/nmea.txt' ;
	
	$Config2->{udp} = { host => '192.168.0.88' }; 
	$Config2->write('fanet.conf', 'utf8'); # Neither ':' nor '>:' prefix!
}

sub GXAirComFNT {
  my( $socket, $data);
  $socket = new IO::Socket::INET (PeerAddr   => '192.168.0.57:10110', Proto        => 'udp') or die "ERROR in Socket Creation : $!\n ";
#send operation
my $send = $_[0];
#FNT type , dest_manufacturer , dest_id , forward ( 0 . . 1 ) , ackrequired ( 0 . . 1 ) ,length , payload ( length ∗2 hex chars )<, signature >\
#"#FNF 1,128,1,0,4,B,263E00468BF405BF000004"
$send =  "FNT 1,2,1234,1,0,B,263E00468BF405BF000004\n";
#not working as message should come over bluetooth, or serial line.
$socket->send($send);
}

sub GETfromUDP2 {
my($sock, $oldmsg, $newmsg, $hisaddr, $hishost, $MAXLEN, $PORTNO);
$MAXLEN = 1024;
$PORTNO = 12390;
$sock = IO::Socket::INET->new(LocalPort => $PORTNO, Proto => 'udp', Blocking => 1)
    or die "socket: $@";

while ($sock->recv($newmsg, $MAXLEN)) {
    my($port, $ipaddr) = sockaddr_in($sock->peername);
    $hishost = gethostbyaddr($ipaddr, AF_INET);
    
   my $row =  $hishost .  " PSRFI,0," . $newmsg . ", 0";
#    print now() . $row;
    &process_line($row, $hishost);
#    print $fh $row;
}
}

sub GETfromUDP {
my($sock, $oldmsg, $newmsg, $hisaddr, $hishost, $MAXLEN, $PORTNO);
$MAXLEN = 1024;
$PORTNO = 10110;
$sock = IO::Socket::INET->new(LocalPort => $PORTNO, Proto => 'udp', Blocking => 1)
    or die "socket: $@";
    my $fh ="";
    if ($Config->{nmea}->{log} eq "yes"){
open($fh, '>>:encoding(UTF-8)', $Config->{nmea}->{path});
	}
while ($sock->recv($newmsg, $MAXLEN)) {
    my($port, $ipaddr) = sockaddr_in($sock->peername);
    $hishost = gethostbyaddr($ipaddr, AF_INET);
    my $row =  $hishost .  " " . $newmsg ;
#    print  gmtime() . " ". $row;
    &process_line($row);
     if ($Config->{nmea}->{log} eq "yes"){
     	 print $fh gmtime() . " ". $row;
     }
}
}

sub GETfromSQL {
  my $where = $_[0];
  $dbh2->do('set names utf8');

  my $examples = $dbh2->prepare(<<'END_SQL');
select * from (  select
    replace(json_unquote(json_extract(`fanet_packet`.`hexString`, '$.rawMsg.hexMsg')), ' ', '') AS `hexMsg`,
    UNIX_TIMESTAMP( `fanet_packet`.`tms`) AS `tms`
from
    `fanet_packet`
where
type = 2 and
substr(device,1,2) in ('00', '01', '03', '04','05', '06', '07', '11', 'E0', 'FB', 'FC', 'FD') and
    json_valid(`fanet_packet`.`hexString`) 
order by tms desc
    limit 0
    ) x 
    union all 
    select * from (
     select
    replace(json_unquote(json_extract(`fanet_packet`.`hexString`, '$.rawMsg.hexMsg')), ' ', '') AS `hexMsg`,
    UNIX_TIMESTAMP( `fanet_packet`.`tms`) AS `tms`
from
    `fanet_packet`
where
type = 1 and
substr(device,1,2) in ('00', '01', '03', '04','05', '06', '07', '11', 'E0', 'FB', 'FC', 'FD') and
    json_valid(`fanet_packet`.`hexString`) 
order by tms desc
    limit 3 ) x 
    union all 
    select * from (
     select
    replace(json_unquote(json_extract(`fanet_packet`.`hexString`, '$.rawMsg.hexMsg')), ' ', '') AS `hexMsg`,
    UNIX_TIMESTAMP( `fanet_packet`.`tms`) AS `tms`
from
    `fanet_packet`
where
type = 4 and
substr(device,1,2) in ('00', '01', '03', '04','05', '06', '07', '11', 'E0', 'FB', 'FC', 'FD') and
    json_valid(`fanet_packet`.`hexString`) 
order by tms desc
    limit 0
        ) x 
    union all 
    select * from ( 
     select
    replace(json_unquote(json_extract(`fanet_packet`.`hexString`, '$.rawMsg.hexMsg')), ' ', '') AS `hexMsg`,
    UNIX_TIMESTAMP( `fanet_packet`.`tms`) AS `tms`
from
    `fanet_packet`
where
type = 7 and
substr(device,1,2) in ('00', '01', '03', '04','05', '06', '07', '11', 'E0', 'FB', 'FC', 'FD') and
    json_valid(`fanet_packet`.`hexString`) 
order by tms desc
    limit 0
            ) x 
    union all 
    select * from (
     select
    replace(json_unquote(json_extract(`fanet_packet`.`hexString`, '$.rawMsg.hexMsg')), ' ', '') AS `hexMsg`,
    UNIX_TIMESTAMP( `fanet_packet`.`tms`) AS `tms`
from
    `fanet_packet`
where
type = 8 and
substr(device,1,2) in ('00', '01', '03', '04','05', '06', '07', '11', 'E0', 'FB', 'FC', 'FD') and
    json_valid(`fanet_packet`.`hexString`) 
order by tms desc
    limit 0
                ) x 
    union all 
    select * from (
     select
    replace(json_unquote(json_extract(`fanet_packet`.`hexString`, '$.rawMsg.hexMsg')), ' ', '') AS `hexMsg`,
    UNIX_TIMESTAMP( `fanet_packet`.`tms`) AS `tms`
from
    `fanet_packet`
where
type = 9 and
substr(device,1,2) in ('00', '01', '03', '04','05', '06', '07', '11', 'E0', 'FB', 'FC', 'FD') and
    json_valid(`fanet_packet`.`hexString`) 
order by tms desc
    limit 0
    )x
END_SQL
$examples->execute();
while (my @row =$examples->fetchrow_array())
{
#print $row[0]."\n";
if ($where eq "db") {
&process_line("PSRFI,$row[1],$row[0],00");
} 

elsif ($where eq "send") {
    print $row[0] ."\n";
  &SENDtoUDP($row[0]);
  sleep 4;
}
}
}

sub GETfromFile {
if (open(my $fh, '<:encoding(UTF-8)', $Config->{nmea}->{path})) {
  while (my $row = <$fh>) {
    &process_line($row, 1)
  }
  } else {
    warn "Could not open file '".$Config->{nmea}->{path}."' $!";
  }
}

sub process_line {
  my    $row = $_[0];
my $filter = 0;
print gmtime() . "  ". $row;
    chomp $row;
	my @values = split(',', $row);
	if ($values[0] =~ /PSRFI/ )
	{
		process_line_PSRFI($row);
	} elsif ($values[0] =~ /FNF/ ) {
process_line_FNF($row, $_[1]);
}
#FNF 1,128,1,0,4,B,263E00468BF405BF000004
}

sub process_line_FNF {
  my    $row = $_[0];
	my @values = split(',', substr($row,5,100));
	#FNF src_manufacturer , src_id , broadcast ( 0..1 ) , signature , type ,length , payload ( length ∗2 hex chars )\n
	#FNF 1                ,128     ,1                   ,0          ,4       ,B,     263E00468BF40	
	#0                      1       2                    3           4        5       6
	my $src_manufacturer = $values[0];
	my $src_id           = $values[1];
	my $broadcast        = $values[2];
	my $signature        = $values[3];
	my $type             = $values[4];
	my $length			 = $values[5];
	my $payload          = $values[6];
	print "#FNF
src_manufacturer   :$src_manufacturer 
src_id             :$src_id
broadcast          :$broadcast        
signature          :$signature        
type               :$type             
length             :$length			 
payload            :$payload          
	 ";

	 $dbh->do('INSERT INTO FANET_Header
(Manufacturer, UniqueID,  `Type` , received_by, Original, `Timestamp`)
VALUES(?, ?, ?, get_Rec_id(?), ?,  SYSDATE())', undef,
$src_manufacturer, $src_id,  $type,   $_[1], $row);
my $id = $dbh->last_insert_id( undef, undef, undef, undef ) or return;

if ($type==1) {
    process_type_1($payload, $id, $src_manufacturer. $src_id);
}
elsif ($type==2) {
	process_type_2($payload, $id, $src_manufacturer. $src_id);
}
elsif ($type==3) {
	process_type_3($payload, $id, $src_manufacturer. $src_id);
}
elsif ($type==4) {
process_type_4($payload, $id, $src_manufacturer. $src_id);
}
elsif ($type==7) {
process_type_7($payload, $id, $src_manufacturer. $src_id);
}
elsif ($type==8) {
process_type_8($payload, $id, $src_manufacturer. $src_id);
}
elsif ($type==9) {
process_type_9($payload, $id, $src_manufacturer. $src_id);
}

}
sub process_line_PSRFI {
	  my    $row = $_[0];
	my @values = split(',', $row);
    my $RFDBI = $values[3];
    my $formatted_time = time2str("%c", $values[1]) ;

	 my( @hex ) = unpack( '(A2)*', $values[2] );
 	 my( $newencoded ) = pack( '(H2)*', @hex );

	 my $ExtendedHeader =   ( hex($hex[0])  >> 7 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 7  ,
     my $Forward        =    ( hex($hex[0])  >> 6 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 6.,
     my $Type           =    ( hex($hex[0])  >> 0 ) & ( 1 << 5 ) - 1;  # 5 bit  starting at bit 0.,
     my $Manufacturer   =    $hex[1];
     my $UniqueID       =    $hex[3].$hex[2];
     my $startmsg       =   4;
      print "RFDBI          $RFDBI
formatted_time $formatted_time
@hex
ExtendedHeader = $ExtendedHeader
Forward        = $Forward
Type           = $Type
Manufacturer   = $Manufacturer
UniqueID       = $UniqueID
";
my ($received_by) = ($values[0] =~ /\A(.*?) /);
#CREATE PROCEDURE citycount (IN country CHAR(3), OUT cities INT)
# insert ...on duplicate keys update
#       BEGIN
#         SELECT COUNT(*) INTO cities FROM world.city
#         WHERE CountryCode = country;
#       END//
#If a table contains an AUTO_INCREMENT column and INSERT ... ON DUPLICATE KEY UPDATE inserts or updates a row, the LAST_INSERT_ID() function returns the AUTO_INCREMENT value.
$dbh->do('INSERT INTO FANET_Header
(Manufacturer, UniqueID, Forward, `Type`, ExtendedHeader, DBI, received_by, Original, `Timestamp`,`Timestamp2`)
VALUES(?, ?, ?, ?, ?, ?, get_Rec_id(?), ?, FROM_UNIXTIME(?), SYSDATE())', undef,
$Manufacturer, $UniqueID, $Forward, $Type, $ExtendedHeader, $RFDBI, $received_by, $values[2], $values[1]);
my $id = $dbh->last_insert_id( undef, undef, undef, undef ) or return;
print("$id\n");

      if ($ExtendedHeader == 1){
         my $ExtendedHeaderACK = ( hex($hex[$startmsg+0])    >> 6 ) & ( 1 << 2 ) - 1;  #2 bit  starting at bit 6  ,
         my $ExtendedHeaderCast = (              hex($hex[$startmsg+0])    >> 5 ) & ( 1 << 1 ) - 1;# 1 bit  starting at bit 5  ,
         my $ExtendedHeaderSignatureBit =(       hex($hex[$startmsg+0])    >> 4 ) & ( 1 << 1 ) - 1;# 1 bit  starting at bit 5  ,
         my $ExtendedHeaderGeobasedForwarded = ( hex($hex[$startmsg+0])    >> 3 ) & ( 1 << 1 ) - 1;# 1 bit  starting at bit 5  ,

          $startmsg = 5;
         print "
         $hex[$startmsg+0]
ExtendedHeaderCast               : $ExtendedHeaderCast
ExtendedHeaderACK                : $ExtendedHeaderACK
ExtendedHeaderSignatureBit       : $ExtendedHeaderSignatureBit
ExtendedHeaderGeobasedForwarded  : $ExtendedHeaderGeobasedForwarded
   ";

$dbh->do('   INSERT INTO FANET_ExtendedHeader
(ExtendedHeaderID, ExtendedHeaderACK, ExtendedHeaderCast, ExtendedHeaderSignatureBit, ExtendedHeaderGeobasedForwarded)
VALUES(?, ?, ?, ?, ?);', undef,
$id,               $ExtendedHeaderACK ,   $ExtendedHeaderCast,  $ExtendedHeaderSignatureBit, $ExtendedHeaderGeobasedForwarded);

         if ($ExtendedHeaderCast == 1){
=begin
=cut
            my $ExtendedHeaderCastDestinationManufacturer;
            my $ExtendedHeaderCastDestinationUniqueID;
             $startmsg = 8;
         }
         if ($ExtendedHeaderSignatureBit == 1){
            my $ExtendedHeaderSignature;
             $startmsg += 3 ;
=begin
=cut
         }
          }

	my $payload     = join('',@hex[$startmsg .. $#hex]);
if ($Type==1) {
    process_type_1($payload, $id, $Manufacturer. $UniqueID);
}
elsif ($Type==2) {
	process_type_2($payload, $id, $Manufacturer. $UniqueID);
}
elsif ($Type==3) {
	process_type_3($payload, $id, $Manufacturer. $UniqueID);
}
elsif ($Type==4) {
process_type_4($payload, $id, $Manufacturer. $UniqueID);
}
elsif ($Type==7) {
process_type_7($payload, $id, $Manufacturer. $UniqueID);
}
elsif ($Type==8) {
process_type_8($payload, $id, $Manufacturer. $UniqueID);
}
elsif ($Type==9) {
process_type_9($payload, $id, $Manufacturer. $UniqueID);
}
}


sub process_type_3 {
	my @hexloc = unpack( '(A2)*', $_[0] );
	my $id     = $_[1];
	my @dec    = map { hex($_) } @hexloc[1 .. $#hexloc-1];
	my @bytes  = map { pack('C', $_) } @dec;
	my $Type3_message = join ('', @bytes);
	my $Type3_header = hex($hexloc[0]);
 print "
 Type3_header  ",$Type3_header,"
 Type3_message ",$Type3_message,"\n";

 $dbh->do('INSERT INTO FANET_TYP_3
(Type3ID, Type3Header, Type3Message)
VALUES(?, ?, ?);
', undef, $id , $Type3_header, $Type3_message);
}


sub process_type_4 {
	my @hex = unpack( '(A2)*', $_[0] );
	my $startmsg = 0;
	my $id     = $_[1];
	 my $Type4Header         = hex($hex[$startmsg+0]);
   my $Type4HeaderInternetGateway   = ( $Type4Header  >> 7 ) & ( 1 <<  1 ) - 1;  # 1 bit  starting at bit 0  ,;;
   my $Type4HeaderTemperature       = ( $Type4Header  >> 6 ) & ( 1 <<  1 ) - 1;
   my $Type4HeaderWind              = ( $Type4Header  >> 5 ) & ( 1 <<  1 ) - 1;
   my $Type4HeaderHumidity          = ( $Type4Header  >> 4 ) & ( 1 <<  1 ) - 1;
   my $Type4HeaderBarometric        = ( $Type4Header  >> 3 ) & ( 1 <<  1 ) - 1;
   my $Type4HeaderSupport           = ( $Type4Header  >> 2 ) & ( 1 <<  1 ) - 1;
   my $Type4HeaderStateCharge       = ( $Type4Header  >> 1 ) & ( 1 <<  1 ) - 1;
   my $Type4HeaderExtendedHeader    = ( $Type4Header  >> 0 ) & ( 1 <<  1 ) - 1;
   my $Latitude        = hex(join('',reverse(@hex[$startmsg+1 .. $startmsg+3]) )   )/93206;
   my $Longitude       = hex(join('',reverse(@hex[$startmsg+4 .. $startmsg+6]) )   )/46603;
   my $Type4InternetGateway   = 0;
   my $Type4Temperature       = 0;
   my $Type4WindHeading = 0;
   my $Type4WindSpeed = 0;
   my $Type4WindGusts    = 0          ;
   my $Type4Humidity        = 0  ;
   my $Type4Barometric      = 0  ;
   my $Type4Support         = 0  ;
   my $Type4StateCharge     = 0  ;
   my $Type4ExtendedHeader  = 0  ;
   $startmsg += 7;
   if ($Type4HeaderInternetGateway   == 1){
    $startmsg += 0;
   }
   if ($Type4HeaderTemperature       == 1){

     my $Type4TemperatureByte = hex($hex[$startmsg]);
     #print ("Type4TemperatureByte  $hex[$startmsg] \n");
     #printf "Type4TemperatureBin   %b\n", hex($hex[$startmsg]);
     my $Type4Temperature2comp = ( $Type4TemperatureByte >> 7 ) & ( 1 << 1 ) - 1;
     #print ("Type4Temperature2comp $Type4Temperature2comColumn1p \n");
     $Type4Temperature  =(-128 * ($Type4Temperature2comp) + (( $Type4TemperatureByte  >> 0 ) & ( 1 << 7 ) - 1  ) ) /2;  # 7 bit  starting at bit 0  ,; #-
    $startmsg += 1;
   }
   if ($Type4HeaderWind              == 1){
     $Type4WindHeading =  hex($hex[$startmsg])/256*360;
     $Type4WindSpeed   = hex($hex[$startmsg+1])/5;
     #keinscale missing
     $Type4WindGusts   = hex($hex[$startmsg+2])/5;
     $startmsg += 3;
   }
   if ($Type4HeaderHumidity          == 1){
        $Type4Humidity =  hex($hex[$startmsg])*4/10;
     $startmsg += 1;
   }
   if ($Type4HeaderBarometric        == 1){
     $Type4Barometric = hex(join('',reverse(@hex[$startmsg+0 .. $startmsg+2]) )   )*10+430;
     $startmsg += 2;
   }
   if ($Type4HeaderSupport           == 1){
     $startmsg += 0;
   }
   if ($Type4HeaderStateCharge       == 1){
     $Type4StateCharge =  hex($hex[$startmsg])/256*100;
     $startmsg += 1;
   }
   if ($Type4HeaderExtendedHeader    == 1){
     $startmsg += 1;
   }
   print "
Type4HeaderInternetGateway   =$Type4HeaderInternetGateway
Type4HeaderTemperature       =$Type4HeaderTemperature
Type4HeaderWind              =$Type4HeaderWind
Type4HeaderHumidity          =$Type4HeaderHumidity
Type4HeaderBarometric        =$Type4HeaderBarometric
Type4HeaderSupport           =$Type4HeaderSupport
Type4HeaderStateCharge       =$Type4HeaderStateCharge
Type4HeaderExtendedHeader    =$Type4HeaderExtendedHeader
Latitude                     =$Latitude
Longitude                    =$Longitude
Type4Temperature           =$Type4Temperature
Type4WindHeading           =$Type4WindHeading
Type4WindSpeed             =$Type4WindSpeed
Type4WindGusts             =$Type4WindGusts
Type4Humidity              =$Type4Humidity
Type4Barometric            =$Type4Barometric
Type4StateCharge           =$Type4StateCharge

https://www.google.com/maps/search/?api=1&query=$Latitude,$Longitude
   ";
   $dbh->do('   INSERT INTO FANET_TYP_4
   (Type4ID,
   Type4HeaderInternetGateway,
   Type4HeaderTemperature,
   Type4HeaderWind,
   Type4HeaderHumidity,
   Type4HeaderBarometric,
   Type4HeaderSupport,
   Type4HeaderStateCharge,
   Type4HeaderExtendedHeader,
   Type4Latitude,
   Type4Longitude,
   Type4Temperature,
   Type4WindHeading,
   Type4WindSpeed,
   Type4WindGusts,
   Type4Barometric,
   Type4Type4Humidity,
   Type4StateCharge)
   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?);
   ', undef,
   $id ,
   $Type4HeaderInternetGateway,
   $Type4HeaderTemperature,
   $Type4HeaderWind,
   $Type4HeaderHumidity,
   $Type4HeaderBarometric,
   $Type4HeaderSupport,
   $Type4HeaderStateCharge,
   $Type4HeaderExtendedHeader,
   $Latitude,
   $Longitude,
   $Type4Temperature,
   $Type4WindHeading,
   $Type4WindSpeed,
   $Type4WindGusts,
   $Type4Barometric,
   $Type4Humidity,
   $Type4StateCharge);


 }
 

sub process_type_7 {
	my @hex = unpack( '(A2)*', $_[0] );
	my $startmsg = 0;
	my $id     = $_[1];
     my $Latitude               = hex(join('',reverse(@hex[$startmsg+0 .. $startmsg+2]) )   )/93206;
     my $Longitude              = hex(join('',reverse(@hex[$startmsg+3 .. $startmsg+5]) )   )/46603;
	 my $Type7_Type_help        = hex($hex[$startmsg+6])    ;
	 my $Type7_Type             = ( $Type7_Type_help >> 4 ) & ( 1 << 3 ) - 1;  # 3 bit  starting at bit 7  ,;
	 my $Type7OnlineTracking    = ( $Type7_Type_help  >> 0 ) & ( 1 <<  1 ) - 1;  # 1 bit  starting at bit 0  ,;;

      print "
Latitude            $Latitude
Longitude           $Longitude
Type7_Type_help     $Type7_Type_help
Type7_Type          $Type7_Type
Type7OnlineTracking $Type7OnlineTracking
https://www.google.com/maps/search/?api=1&query=$Latitude,$Longitude
 ";
  $dbh->do('INSERT INTO FANET_TYP_7
 (Type7ID, Type7Latitude, Type7Longitude, Type7Type, Type7OnlineTracking)
 VALUES(?, ?, ?, ?, ?);', undef,
 $id, $Latitude, $Longitude, $Type7_Type, $Type7OnlineTracking);

  if ($Config->{mysql2}->{send_data} eq "yes"){
$dbh2->do('INSERT INTO fanet_track
      (device                   , tms              , received_by, `type`         , lat     , lon       , tracking         ,   vspd,   distance)
VALUES(?                        , CURRENT_TIMESTAMP, "DANNSTADT",      ?         ,   ?     ,   ?       ,       ?          ,      0,     ?);', undef, 
        $_[2] ,                               ,  $Type7_Type ,$Latitude, $Longitude,$Type7OnlineTracking,         $geo->distance(mile => $Longitude, $Latitude  => 8.314746, 49.421713) * 1.60934) or print  "Can't prepare SQL statement: $DBI::errstr\n"; 
}

 }
sub process_type_8 {
	my @hex = unpack( '(A2)*', $_[0] );
	my $startmsg = 0;
	my $id     = $_[1];
  my $DeviceType          = hex($hex[$startmsg])    ;
  my $FirmwareHelp        = hex(join('',reverse(@hex[$startmsg+1 .. $startmsg+2])))    ;
  print ("FirmwareHelpBIN   $FirmwareHelp");
  my $FirmwareRelease     = ( $FirmwareHelp    >> 16 ) & ( 1 << 1 ) - 1;
  my $FirmwareReleaseyear =  (( $FirmwareHelp   >> 9  ) & (( 1 << 5 ) - 1)) + 2019 ;
  my $FirmwareReleasemonth = (( $FirmwareHelp   >> 5  ) & (( 1 << 3 ) - 1));
  my $FirmwareReleaseday   = (( $FirmwareHelp   >> 0  ) & (( 1 << 4 ) - 1)) ;
  my $FirmwareReleaseDate   = $FirmwareReleaseyear * 10000 + $FirmwareReleasemonth*100+$FirmwareReleaseday;
  my $FirmwareReleaseDatestr = $FirmwareReleaseyear ."-". $FirmwareReleasemonth."-".$FirmwareReleaseday;
  print "
DeviceType           $DeviceType
FirmwareHelp         $FirmwareHelp
FirmwareRelease      $FirmwareRelease
FirmwareReleaseDate   $FirmwareReleaseDate
FirmwareReleaseDatestr   $FirmwareReleaseDatestr
";

  $dbh->do('INSERT INTO FANET_TYP_8
  (Type8ID, Type8DeviceType, Type8FirmwareRelease, Type8FirmwareReleaseDate)
VALUES(?, ?, ?, STR_TO_DATE(?, "%Y-%m-%d"));', undef,
$id,
$DeviceType,
$FirmwareRelease,
$FirmwareReleaseDatestr);


}
sub process_type_9{
	my @hex = unpack( '(A2)*', $_[0] );
	my $startmsg = 0;
	my $id     = $_[1];
     my $Latitude               = hex(join('',reverse(@hex[$startmsg+0 .. $startmsg+2]) )   )/93206;
     my $Longitude              = hex(join('',reverse(@hex[$startmsg+3 .. $startmsg+5]) )   )/46603;
     my $Type1_Type      = hex(join('',reverse(@hex[$startmsg+6 .. $startmsg+7]) )   );
     my $confidence    = ((( $Type1_Type  >> 12 ) & ( 1 << 3 ) - 1))/7*100;  # 3 bit  starting at bit 12  ,;;
     my $ThermalAltitude        = ( $Type1_Type  >> 0 ) & ( 1 << 11 ) - 1;
     my $ThermalAltitudeScaling = ( $Type1_Type  >> 11 ) & ( 1 << 1 ) - 1 ;
     if  ( $ThermalAltitudeScaling               == 1) {
        $ThermalAltitude        = $ThermalAltitude * 4;
       }
      my $AvgClimb_help    = hex($hex[$startmsg+8]);
      my $AvgClimb_Scaling  = ( $AvgClimb_help  >> 7 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 7  ,;
      my $AvgClimb = (( $AvgClimb_help  >> 0 ) & ( 1 << 7 ) - 1 )/10;  # 7 bit  starting at bit 0  ,;
      if ($AvgClimb_Scaling ==1){
      $AvgClimb  = $AvgClimb  * 5;
      }
      my $AvgWind_help    = hex($hex[$startmsg+9]);
      my $AvgWind_Scaling  = ( $AvgWind_help  >> 7 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 7  ,;
      my $AvgWind = (( $AvgWind_help  >> 0 ) & ( 1 << 7 ) - 1 )/2;  # 7 bit  starting at bit 0  ,;
      if ($AvgWind_Scaling ==1){
      $AvgWind  = $AvgWind  * 5;
      }
 my $WindHeading = hex($hex[$startmsg+10])/256*360;
 print "
Latitude          $Latitude
Longitude         $Longitude
confidence        $confidence
ThermalAltitude   $ThermalAltitude
AvgClimb          $AvgClimb
AvgWind           $AvgWind
WindHeading       $WindHeading
https://www.google.com/maps/search/?api=1&query=$Latitude,$Longitude
 ";
 $dbh->do('INSERT INTO FANET_TYP_9
(Type9ID, Type9Latitude, Type9Longitude, Type9Confidence, Type9ThermalAltitude, Type9AvgClimb, Type9AvgWind, Type9WindHeading)
VALUES(?, ?, ?, ?, ?, ?, ?, ?);
', undef,
$id,
$Latitude,
$Longitude,
$confidence,
$ThermalAltitude,
$AvgClimb,
$AvgWind,
$WindHeading
);
}


sub process_type_2 {
	my @hex = unpack( '(A2)*', $_[0] );
	my $startmsg = 0;
	my $id     = $_[1];
@hex    = @hex[$startmsg+0 .. $#hex-1];
my @dec    = map { hex($_) } @hex;
my @bytes  = map { pack('C', $_) } @dec;
my $Type2_name = join ('', @bytes);	
my $Type2_name_utf8 = decode("UTF-8", $Type2_name);
#utf8::upgrade($Type2_name);
#print utf8::is_utf8($Type2_name);

 print ("Type2_name $Type2_name\n");
 # Blaettersbe
 $dbh->do('INSERT INTO FANET_TYP_2
 (Type2ID, Type2Name)
 VALUES(?, ?);', undef, $id ,  $Type2_name);
#$dbh->do('REPLACE INTO  FANET_NAME (Manufacturer, UniqueID, NAME) VALUES(?, ?, ?) ;', {}, $Manufacturer, $UniqueID ,  $Type2_name);
$dbh->do('REPLACE INTO  FANET_NAME (Manufacturer, UniqueID, NAME) VALUES(?, ?, ?) ;', {}, substr($_[2],1,2),  substr($_[2],3,8) ,  $Type2_name_utf8);
}

sub process_type_1 {
	my @hex = unpack( '(A2)*', $_[0] );
	my $startmsg = 0;
	my $id     = $_[1];
	#splice my @slice = @rainbow[0 .. $#rainbow - 5];
     my $Latitude        = hex(join('',reverse(@hex[$startmsg+0 .. $startmsg+2]) )   )/93206;
     my $Longitude       = hex(join('',reverse(@hex[$startmsg+3 .. $startmsg+5]) )   )/46603;
	 my $Type1_Type      = hex(join('',reverse(@hex[$startmsg+6 .. $startmsg+7]) )   );
	 my $OnlineTracking  = ( $Type1_Type  >> 15 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 15  ,;
	 my $AircraftType    = ( $Type1_Type  >> 12 ) & ( 1 << 3 ) - 1;  # 3 bit  starting at bit 12  ,;;
     my $Altitude        = ( $Type1_Type  >> 0 ) & ( 1 << 11 ) - 1;
     my $AltitudeScaling = ( $Type1_Type  >> 11 ) & ( 1 << 1 ) - 1 ;
     if  ( $AltitudeScaling               == 1) {
      $Altitude        = $Altitude * 4;
     }
     my $Speed_help    = hex($hex[$startmsg+8]);
     my $Speed_Scaling  = ( $Speed_help  >> 7 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 7  ,;
     my $Speed  = (( $Speed_help  >> 0 ) & ( 1 << 7 ) - 1 )/2;  # 7 bit  starting at bit 0  ,;
     if ($Speed_Scaling ==1){
     $Speed  = $Speed  * 5;
     }

      my $Climb_help    = hex($hex[$startmsg+9]);
     my $Climb_Scaling  = ( $Climb_help  >> 7 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 7  ,;
     my $Climb_2comple  = ( $Climb_help  >> 6 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 6  ,;
     my $Climb  =(-64 * $Climb_2comple + (( $Climb_help  >> 0 ) & ( 1 << 6 ) - 1  ) ) /10;  # 6 bit  starting at bit 0  ,; #-
     if ($Climb_Scaling ==1){
     $Climb  = $Climb  * 5;
     }

     my $Heading = hex($hex[$startmsg+10])/256*360;
      my $TurnRate  = 0; #TurnRate is optional
if (($#hex - $startmsg)>=11) {
      my $TurnRate_help    = hex($hex[$startmsg+11]);
     my $TurnRate_Scaling  = ( $TurnRate_help  >> 7 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 7  ,;
     my $TurnRate_2comple  = ( $TurnRate_help  >> 6 ) & ( 1 << 1 ) - 1;  # 1 bit  starting at bit 6  ,;
     $TurnRate  =(-64 * $TurnRate_2comple + (( $TurnRate_help  >> 0 ) & ( 1 << 6 ) - 1  ) ) /10;  # 6 bit  starting at bit 0  ,; #-
     if ($TurnRate_Scaling ==0){
     $TurnRate  = $TurnRate  /4;
     }
}

     #_GPS_KMPH_PER_KNOT 1.852  m TXRX_TEST_SPEED       50.0 = 92.5 = OK
     print "Latitude        $Latitude
Longitude       $Longitude
OnlineTracking  $OnlineTracking
AircraftType    $AircraftType
Altitude        $Altitude
Speed           $Speed  Speed_Scaling$Speed_Scaling
Climb           $Climb  Climb_Scaling$Climb_Scaling Climb_2comple$Climb_2comple
Heading        $Heading
TurnRate       $TurnRate
https://www.google.com/maps/search/?api=1&query=$Latitude,$Longitude
";

$dbh->do('INSERT INTO FANET_TYP_1
(Type1ID, Type1Latitude, Type1Longitude, Type1OnlineTracking, Type1AircraftType, Type1Altitude, Type1Speed, Type1Climb, Type1Heading, Type1TurnRate)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', undef, $id ,  $Latitude, $Longitude, $OnlineTracking,  $AircraftType, $Altitude, $Speed, $Climb, $Heading, $TurnRate) ;
if ($Config->{mysql2}->{send_data} eq "yes"){
$dbh2->do('INSERT INTO fanet_track
      (device                   , tms              , received_by, `type`         , lat     , lon       , tracking      , alt        , speed , course   , vspd , trate    , distance)
VALUES(?                        , CURRENT_TIMESTAMP, "DANNSTADT",      ?         ,   ?     ,   ?       ,       ?       ,    ?       ,   ?   ,   ?      , ?    , ?        ,        ?);', undef, 
        $_[2],                               ,  $AircraftType ,$Latitude, $Longitude,$OnlineTracking,   $Altitude, $Speed,  $Heading,$Climb, $TurnRate, $geo->distance(mile => $Longitude, $Latitude  => 8.314746, 49.421713) * 1.60934)  ;
}
}


sub remove_and_create_tables{
$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_ExtendedHeader`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_ExtendedHeader` (
  `ExtendedHeaderID` bigint(20) NOT NULL,
  `ExtendedHeaderACK` smallint(5) unsigned DEFAULT NULL,
  `ExtendedHeaderCast` smallint(5) unsigned DEFAULT NULL,
  `ExtendedHeaderSignatureBit` TINYINT(1) DEFAULT NULL,
  `ExtendedHeaderGeobasedForwarded` TINYINT(1) DEFAULT NULL,
  PRIMARY KEY (`ExtendedHeaderID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL

$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_Signature`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_Signature` (
  `SignatureID` bigint(20) NOT NULL,
  `Signature` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`SignatureID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL


$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_Header`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_Header` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `Manufacturer` varchar(2) DEFAULT NULL,
  `UniqueID` varchar(6) COLLATE latin1_general_ci DEFAULT NULL,
  `Forward` TINYINT(1) DEFAULT NULL,
  `Type` int(11) DEFAULT NULL,
  `ExtendedHeader` TINYINT(1) DEFAULT NULL,
  `DBI` int(11) DEFAULT NULL,
  `received_by` varchar(100) COLLATE latin1_general_ci DEFAULT NULL,
  `Original` varchar(999) COLLATE latin1_general_ci DEFAULT NULL,
  `Timestamp` datetime DEFAULT NULL,
  `Timestamp2` datetime DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL

$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_received_by`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_received_by` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `received_by` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `FANET_received_by_UN` (`received_by`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL

$dbh->do( <<'END_SQL');
CREATE or replace FUNCTION get_Rec_id (receivedby CHAR(100)) or die "Can't connect to database: $DBI::errstr!\"; docstore.mik.ua/orelly/linux/dbi/ch04_05.htm
    RETURNS BIGINT
       BEGIN
	    insert ignore into FANET_received_by (received_by) values (receivedby) ;
        return (SELECT id    FROM FANET_received_by where received_by = receivedby COLLATE latin1_general_cs) ;
END
END_SQL

$dbh->do( <<'END_SQL');
ALTER TABLE FANET_Header ADD CONSTRAINT FANET_Header_UN UNIQUE KEY (Manufacturer,UniqueID,`Type`, ID);
END_SQL

$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_TYP_1`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_TYP_1` (
  `Type1ID` bigint(20) NOT NULL,
  `Type1Latitude` double DEFAULT NULL,
  `Type1Longitude` double DEFAULT NULL,
  `Type1OnlineTracking` tinyint(1) DEFAULT NULL,
  `Type1AircraftType` TINYINT(6) DEFAULT NULL,
  `Type1Altitude` int(10) unsigned DEFAULT NULL,
  `Type1Speed` int(10) unsigned DEFAULT NULL,
  `Type1Climb` int(11) DEFAULT NULL,
  `Type1Heading` TINYINT(6) DEFAULT NULL,
  `Type1TurnRate` int(11) DEFAULT NULL,
  PRIMARY KEY (`Type1ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL


$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_TYP_2`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_TYP_2` (
  `Type2ID` bigint(20) NOT NULL,
  `Type2Name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`Type2ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
END_SQL

$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_NAME`
END_SQL

$dbh->do( <<'END_SQL');
CREATE  TABLE `FANET_NAME` (
`Manufacturer` varchar(2) NOT NULL,
`UniqueID` varchar(6) COLLATE latin1_general_ci NOT NULL,
  `NAME` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`Manufacturer`, UniqueID )
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
END_SQL


$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_TYP_3`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_TYP_3` (
  `Type3ID` bigint(20) NOT NULL,
  `Type3Header` int(11) DEFAULT NULL,
  `Type3Message` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`Type3ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
END_SQL


$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_TYP_4`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_TYP_4` (
  `Type4ID` bigint(20) NOT NULL,
  `Type4Latitude` double DEFAULT NULL,
  `Type4Longitude` double DEFAULT NULL,
  `Type4HeaderInternetGateway` tinyint(1) DEFAULT NULL,
  `Type4HeaderTemperature` tinyint(1) DEFAULT NULL,
  `Type4HeaderWind` tinyint(1) DEFAULT NULL,
  `Type4HeaderHumidity` tinyint(1) DEFAULT NULL,
  `Type4HeaderBarometric` tinyint(1) DEFAULT NULL,
  `Type4HeaderSupport` tinyint(1) DEFAULT NULL,
  `Type4HeaderStateCharge` tinyint(1) DEFAULT NULL,
  `Type4HeaderExtendedHeader` tinyint(1) DEFAULT NULL,
  `Type4Temperature` float DEFAULT NULL,
  `Type4WindHeading` int(11) DEFAULT NULL,
  `Type4WindSpeed` int(11) DEFAULT NULL,
  `Type4WindGusts` int(11) DEFAULT NULL,
  `Type4Barometric` int(11) DEFAULT NULL,
  `Type4Type4Humidity` float DEFAULT NULL,
  `Type4StateCharge` int(11) DEFAULT NULL,
  PRIMARY KEY (`Type4ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL


$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_TYP_7`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_TYP_7` (
  `Type7ID` bigint(20) NOT NULL,
  `Type7Latitude` double DEFAULT NULL,
  `Type7Longitude` double DEFAULT NULL,
  `Type7Type` int(11) DEFAULT NULL,
  `Type7OnlineTracking` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`Type7ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL


$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_TYP_8`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_TYP_8` (
  `Type8ID` bigint(20) NOT NULL,
  `Type8DeviceType` int(11) DEFAULT NULL,
  `Type8FirmwareRelease` tinyint(1) DEFAULT NULL,
  `Type8FirmwareReleaseDate` date DEFAULT NULL,
  PRIMARY KEY (`Type8ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL


$dbh->do( <<'END_SQL');
DROP TABLE IF EXISTS `FANET_TYP_9`
END_SQL

$dbh->do( <<'END_SQL');
CREATE TABLE `FANET_TYP_9` (
  `Type9ID` bigint(20) NOT NULL,
  `Type9Latitude` double DEFAULT NULL,
  `Type9Longitude` double DEFAULT NULL,
  `Type9Confidence` float DEFAULT NULL,
  `Type9ThermalAltitude` int(10) unsigned DEFAULT NULL,
  `Type9AvgClimb` float DEFAULT NULL,
  `Type9AvgWind` float DEFAULT NULL,
  `Type9WindHeading` int(11) DEFAULT NULL,
    PRIMARY KEY (`Type9ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
END_SQL

$dbh->do( <<'END_SQL');
Create or replace View FANET_ALL_Messages as
SELECT FANET_Header.*, FANET_ExtendedHeader.*, FANET_Signature.*,
FANET_NAME.NAME,
FANET_TYP_1.*,
FANET_TYP_2.*,
FANET_TYP_3.*,
FANET_TYP_4.*,
FANET_TYP_7.*,
FANET_TYP_8.*,
FANET_TYP_9.*
FROM FANET_Header
left outer join FANET_NAME on FANET_Header.UniqueID = FANET_NAME.UniqueID and FANET_Header.Manufacturer = FANET_NAME.Manufacturer
left outer join FANET_ExtendedHeader on ExtendedHeaderID = ID
left outer join FANET_Signature      on ID = SignatureID
left outer join FANET_TYP_1          on Type1ID = ID
left outer join FANET_TYP_2 ON ID = Type2ID
left outer join FANET_TYP_3 ON ID = Type3ID
left outer join FANET_TYP_4 ON ID = Type4ID
left outer join FANET_TYP_7 ON ID = Type7ID
left outer join FANET_TYP_8 ON ID = Type8ID
left outer join FANET_TYP_9 ON ID = Type9ID
END_SQL

$dbh->do( <<'END_SQL');
Create or replace View ALL_Messages_not_own as
SELECT FANET_ALL_Messages.*
FROM FANET_ALL_Messages
where UniqueID not in ('8cc4', 'a72c')
order by id desc
END_SQL

$dbh->do( <<'END_SQL');
Create or replace View ALL_Messages_overview as
SELECT  Manufacturer, name,UniqueID, suM(1)
FROM FANET_ALL_Messages
group by Manufacturer, name,UniqueID
END_SQL
}


