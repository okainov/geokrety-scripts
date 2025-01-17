#!/usr/bin/env php
<?php
//śćńółźć

include_once("../../konfig-tools.php");
include_once("$geokrety_www/templates/konfig.php");
require_once "$geokrety_www/__sentry.php";

$link = DBPConnect();


//$BAZY[] = "http://wpg.alleycat.pl/allwps.php";
//$BAZY[] = "gcsu.xml";

$BAZY[] = 'http://www.geocaching.su/rss/geokrety/api.php?interval=25h&changed=1';
//$BAZY[] = 'http://www.geocaching.su/rss/geokrety/api.php?interval=3000d&changed=1';

foreach ($BAZY as $baza) {
    $xml_raw = mb_convert_encoding(strtr(file_get_contents($baza), array('&' => '+')), "UTF-8", "windows-1251");

    $xml = simplexml_load_string($xml_raw, "SimpleXMLElement", LIBXML_NOENT | LIBXML_NOCDATA);

    //print_r($xml); die();

    /*
    <data>
    <cache id="11534">
    <code>EV11534</code>
    <autor>Maxim&Sveta</autor>
    <name>Правдивая история открытия Америки</name>
    <position lat="60.3817333" lon="29.5604000"/></cache>

    */


    foreach ($xml->cache as $cache) {
        $name = trim(mysqli_escape_string($link, strtr((string) $cache->name, array('"' => ''))));
        $lat = (string) $cache->position['lat'];
        $lon = (string) $cache->position['lon'];
        $owner = trim(mysqli_escape_string($link, strtr((string) $cache->autor, array('"' => ''))));
        $waypoint = (string) $cache->code;

        $linka = 'http://www.geocaching.su/?pn=101&cid=' . substr($waypoint, 2, 10);

        //echo "N $lat, E$lon, name: $name, WPT: $wpt L: $linka\n\n";

        $sql = "INSERT INTO `gk-waypointy` ( `waypoint`, `lat` , `lon` , `name`, `link`, `status`, `owner` )
VALUES ('$waypoint',  '$lat', '$lon', '$name', '$linka', '1', '$owner')
ON DUPLICATE KEY UPDATE `waypoint`='$waypoint', `lat`='$lat', `lon`='$lon', `name`='$name', `link`='$linka', `status`='1', `owner`='$owner'";

        //echo "$sql\n\n";
        $result = mysqli_query($link, $sql) or die("error 1: $id $sql");
    }
}

file_put_contents("data-xml.txt", "gksu;" . date("Y-m-d H:i:s"));
mysqli_close($link);
?>
