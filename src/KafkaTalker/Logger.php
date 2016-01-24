<?php
namespace KafkaTalker;

class Logger
{
    private static $call = 'printf';
    private static $debug = false;
    private static $ensureNewLine = true;

    public static function log($message, ...$vars)
    {
        if (self::$debug) {
            $out = sprintf($message, ...$vars);
            if (self::$ensureNewLine) {
                $out .= "\n";
            }
            call_user_func(self::$call, $out);
        }
    }

    public static function setDebug($debug)
    {
        self::$debug = $debug;
    }

    public static function setLogCall($call, $ensureNewLine = false)
    {
        self::$ensureNewLine = $ensureNewLine;
        self::$call = $call;
    }
}
