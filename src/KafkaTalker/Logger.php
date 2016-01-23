<?php
namespace KafkaTalker;

class Logger
{
    private static $debug = false;

    public static function log($message, ...$vars)
    {
        if (self::$debug) {
            printf($message . "\n", ...$vars);
        }
    }

    public static function setDebug($debug)
    {
        self::$debug = $debug;
    }
}
