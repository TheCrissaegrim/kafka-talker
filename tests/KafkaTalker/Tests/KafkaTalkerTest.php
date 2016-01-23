<?php
namespace KafkaTalker\Tests;

use KafkaTalker\Client;
use KafkaTalker\Logger;
use KafkaTalker\Request\DescribeGroupsRequest;

abstract class KafkaTalkerTest extends \PHPUnit_Framework_TestCase
{
    protected $host = 'localhost';
    protected $port = 9092;

    public static function setUpBeforeClass()
    {
        Logger::setDebug(false);
    }

    public function setUp()
    {
        parent::setUp();
    }
}
