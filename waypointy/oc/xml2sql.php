#!/usr/bin/env php
<?php


include_once("../../konfig-tools.php");
//include_once("$geokrety_www/templates/konfig.php");
//require_once "$geokrety_www/__sentry.php";

// For local tests
function DBPConnect()
{
    return mysqli_connect('localhost', 'root', '', 'geokrety-db');
}

function deleteTree($dir)
{
    $files = array_diff(scandir($dir), array('.', '..'));
    foreach ($files as $file) {
        (is_dir("$dir/$file")) ? deleteTree("$dir/$file") : unlink("$dir/$file");
    }
    return rmdir($dir);
}

function downloadFile($url, $output)
{
    $readableStream = fopen($url, 'rb');
    if ($readableStream === false) {
        throw new Exception("Something went wrong while fetching " . $url);
    }
    $writableStream = fopen($output, 'wb');

    stream_copy_to_stream($readableStream, $writableStream);

    fclose($writableStream);
}

function performIncrementalUpdate($link, $changes)
{
    $nbInsertOrUpdate = 0;

    foreach ($changes as $change) {

        if ($change->object_type != 'geocache') {
            continue;
        }

        $id = $change->object_key->code;

        if ($change->change_type == 'delete') {
            //delete from DB

            $sql = 'DELETE FROM `gk-waypointy` WHERE waypoint="' . mysqli_real_escape_string($link, $id) . '"';

            $result = mysqli_query($link, $sql);

            if ($result === false) { // ooooPs we got an import error !
                print("error sql : id:$id - query:$sql - mysqli_error:" . mysqli_error($link) . "\n");
            }
            continue;
        }

        // Check for needed fields and make an update
        $sqlInsert = array();
        $sqlValues = array();
        $sqlUpdate = array();

        if (isset($change->data->names)) {
            $name = mysqli_real_escape_string($link, implode(' | ', (array)$change->data->names));
            $sqlInsert [] = 'name';
            $sqlValues [] = "'$name'";
            $sqlUpdate [] = "name='$name'";
        }
        if (isset($change->data->owner->username)) {
            $owner = mysqli_real_escape_string($link, (string)$change->data->owner->username);
            $sqlInsert [] = 'owner';
            $sqlValues [] = "'$owner'";
            $sqlUpdate [] = "owner='$owner'";
        }
        if (isset($change->data->location)) {
            $location = explode('|', $change->data->location);
            $lon = mysqli_real_escape_string($link, (string)$location[0]);
            $lat = mysqli_real_escape_string($link, (string)$location[1]);

            $sqlInsert [] = 'lon';
            $sqlValues [] = "'$lon'";
            $sqlUpdate [] = "lon='$lon'";

            $sqlInsert [] = 'lat';
            $sqlValues [] = "'$lat'";
            $sqlUpdate [] = "lat='$lat'";
        }
        if (isset($change->data->type)) {
            $type = mysqli_real_escape_string($link, (string)$change->data->type);
            $sqlInsert [] = 'typ';
            $sqlValues [] = "'$type'";
            $sqlUpdate [] = "typ='$type'";
        }
        if (isset($change->data->country)) {
            $country = mysqli_real_escape_string($link, (string)$change->data->country);
            $sqlInsert [] = 'kraj';
            $sqlValues [] = "'$country'";
            $sqlUpdate [] = "kraj='$country'";
        }
        if (isset($change->data->url)) {
            $url = mysqli_real_escape_string($link, (string)$change->data->url);
            $sqlInsert [] = 'link';
            $sqlValues [] = "'$url'";
            $sqlUpdate [] = "link='$url'";
        }

        if (sizeof($sqlInsert) > 0) {
            // It can happen that changelog contains useless fields like "founds" which we are not interested in.
            // So we need to trigger actual update only if at least one of our fields was changes.

            $sqlInsert [] = 'waypoint';
            $sqlValues [] = "'$id'";
            $sqlUpdate [] = "waypoint='$id'";

            $insertPart = '(' . implode(',', $sqlInsert) . ')';
            $valuesPart = '(' . implode(',', $sqlValues) . ')';
            $onDupPart = implode(',', $sqlUpdate);
            $sql = 'INSERT INTO `gk-waypointy` ' . $insertPart . ' VALUES ' . $valuesPart . ' ON DUPLICATE KEY UPDATE ' . $onDupPart;

            $result = mysqli_query($link, $sql);

            if ($result === false) { // ooooPs we got an import error !
                print("error sql : id:$id - query:$sql - mysqli_error:" . mysqli_error($link) . "\n");
            }

            $nbInsertOrUpdate++;
            if ($nbInsertOrUpdate % 500 == 0) {
                echo " o $nbInsertOrUpdate\n";
            }
        }
    }
}


function insertFromFullDump($link, $folder)
{
    $index = json_decode(file_get_contents($folder . '/index.json'));
    $revision = $index->revision;

    foreach ($index->data_files as $piece) {
        $changes = json_decode(file_get_contents($folder . '/' . $piece));
        performIncrementalUpdate($link, $changes);
    }
    return $revision;
}

function getLastUpdate($service)
{
    $link = DBPConnect();
    $sql = "SELECT `last_update` FROM `gk-waypointy-sync` WHERE `service_id` LIKE '" . $service . "%'";
    $result = mysqli_query($link, $sql);
    $row = mysqli_fetch_assoc($result);
    if ($row === null) {
        // Seems like no such key is present. Let's add it
        $sql = "INSERT INTO `gk-waypointy-sync` (service_id) VALUES ('" . $service . "')";
        mysqli_query($link, $sql);
        $row = ['last_update' => null];
    }
    mysqli_close($link);
    return $row['last_update'];
}

function setLastUpdate($service, $lastUpdate)
{
    $link = DBPConnect();
    $sql = "UPDATE `gk-waypointy-sync` SET `last_update`='" . mysqli_real_escape_string($link, $lastUpdate) . "' WHERE `service_id` LIKE '" . $service . "%'";
    mysqli_query($link, $sql);
}


if (getenv("OC_PL_OKAPI_CONSUMER_KEY")) {
    $BAZY_OC['OC_PL']['key'] = getenv("OC_PL_OKAPI_CONSUMER_KEY");
    $BAZY_OC['OC_PL']['url'] = "https://opencaching.pl/okapi/services/replicate/changelog?consumer_key=" . $BAZY_OC['OC_PL']['key'] . "&since=";
    $BAZY_OC['OC_PL']['full_url'] = "https://opencaching.pl/okapi/services/replicate/fulldump?pleeaase=true&consumer_key=" . $BAZY_OC['OC_PL']['key'];
// Also can be local path
#$BAZY_OC['OC_PL']['full_url'] = "C:\Users\Downloads\okapi-dump-r7198164.tar.bz2";
}

if (getenv("OC_DE_OKAPI_CONSUMER_KEY")) {
    $BAZY_OC['OC_DE']['key'] = getenv("OC_DE_OKAPI_CONSUMER_KEY");
    $BAZY_OC['OC_DE']['url'] = "https://www.opencaching.de/okapi/services/replicate/changelog?consumer_key=" . $BAZY_OC['OC_DE']['key'] . "&since=";
    $BAZY_OC['OC_DE']['full_url'] = "https://www.opencaching.de/okapi/services/replicate/fulldump?pleeaase=true&consumer_key=" . $BAZY_OC['OC_DE']['key'];
}

if (getenv("OC_UK_OKAPI_CONSUMER_KEY")) {
    $BAZY_OC['OC_UK']['key'] = getenv("OC_UK_OKAPI_CONSUMER_KEY");
    $BAZY_OC['OC_UK']['url'] = "https://www.opencache.uk/okapi/services/replicate/changelog?consumer_key=" . $BAZY_OC['OC_UK']['key'] . "&since=";
    $BAZY_OC['OC_UK']['full_url'] = "https://www.opencache.uk/okapi/services/replicate/fulldump?pleeaase=true&consumer_key=" . $BAZY_OC['OC_UK']['key'];
}

if (getenv("OC_US_OKAPI_CONSUMER_KEY")) {
    $BAZY_OC['OC_US']['key'] = getenv("OC_US_OKAPI_CONSUMER_KEY");
    $BAZY_OC['OC_US']['url'] = "http://www.opencaching.us/okapi/services/replicate/changelog?consumer_key=" . $BAZY_OC['OC_US']['key'] . "&since=";
    $BAZY_OC['OC_US']['full_url'] = "http://www.opencaching.us/okapi/services/replicate/fulldump?pleeaase=true&consumer_key=" . $BAZY_OC['OC_US']['key'];
}

if (getenv("OC_NL_OKAPI_CONSUMER_KEY")) {
    $BAZY_OC['OC_NL']['key'] = getenv("OC_NL_OKAPI_CONSUMER_KEY");
    $BAZY_OC['OC_NL']['url'] = "https://www.opencaching.nl/okapi/services/replicate/changelog?consumer_key=" . $BAZY_OC['OC_NL']['key'] . "&since=";
    $BAZY_OC['OC_NL']['full_url'] = "https://www.opencaching.nl/okapi/services/replicate/fulldump?pleeaase=true&consumer_key=" . $BAZY_OC['OC_NL']['key'];
}

if (getenv("OC_RO_OKAPI_CONSUMER_KEY")) {
    $BAZY_OC['OC_RO']['key'] = getenv("OC_RO_OKAPI_CONSUMER_KEY");
    $BAZY_OC['OC_RO']['url'] = "https://www.opencaching.ro/okapi/services/replicate/changelog?consumer_key=" . $BAZY_OC['OC_RO']['key'] . "&since=";
    $BAZY_OC['OC_RO']['full_url'] = "https://www.opencaching.ro/okapi/services/replicate/fulldump?pleeaase=true&consumer_key=" . $BAZY_OC['OC_RO']['key'];
}

/*
// Czech OC is super old :(
$BAZY_OC['cz']['prefix'] = 'OZ';
$BAZY_OC['cz']['url'] = "/home/geokrety/public_html/tools/oc-cz.xml";
$BAZY_OC['cz']['szukaj'] = 'http://www.opencaching.cz/searchplugin.php?userinput=';
*/


$totalUpdated = 0;
$totalErrors = 0;

foreach ($BAZY_OC as $key => $baza) {
    $nbImported = 0;
    $nbInsertOrUpdate = 0;
    $nbError = 0;

    if (isset ($GLOBALS['argv'][1]) && $GLOBALS['argv'][1] == 'full') {
        $full_resync = true;
    } else {
        $lastUpdate = getLastUpdate($key);
        if (!empty($lastUpdate)) {
            $baza['url'] .= $lastUpdate;
            $full_resync = false;
        } else {
            $full_resync = true;
        }
    }

    if ($full_resync) {
        try {
            $temp = "tempfile.tar.bz2";
            downloadFile($baza['full_url'], $temp);
            // full dump gives .tar.bz2 file...

            // Save bz2 file
            $bz = bzopen($temp, "r");
            $res = fopen("tempfile.tar", "w");
            $copied = stream_copy_to_stream($bz, $res);
            fclose($res);
            fclose($bz);

            // Extract tar
            //For at least .pl Phar complains that archive is corrupted :(
            // Maybe use exec on lin?
            //exec('mkdir TestBlable && tar -C TestBlable -xvf tempfile.tar');

            $phar = new PharData('tempfile.tar');
            $phar->extractTo('TestBlable'); // extract all files

            // Finally we got 2+ Gb of random json files. Let's parse
            $path = 'TestBlable';
        } catch (Exception $e) {
            print("Cannot download full dump for " . $key);
            print($e->getMessage());
            continue;
        }

        $link = DBPConnect();
        $revision = insertFromFullDump($link, $path);
        mysqli_close($link);
        deleteTree($path);
    } else {
        $raw = file_get_contents($baza['url']);
        $json = json_decode($raw);
        $changes = $json->changelog;

        echo " * processing " . $key . "\n";
        echo " *      count:" . sizeof($changes) . "\n";
        echo " *        url:" . $baza['url'] . "\n";

        if (sizeof($changes) > 0) {
            $link = DBPConnect();
            performIncrementalUpdate($link, $changes);
            mysqli_close($link);
        }
        $revision = $json->revision;
    }
    setLastUpdate($key, $revision);
}

?>
