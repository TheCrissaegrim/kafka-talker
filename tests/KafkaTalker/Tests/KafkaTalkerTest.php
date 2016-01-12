<?php
namespace KafkaTalker\Tests;

use KafkaTalker\Client;
use KafkaTalker\Request\DescribeGroupsRequest;

abstract class KafkaTalkerTest extends \PHPUnit_Framework_TestCase
{
    protected $debug = false;
    protected $host = 'localhost';
    protected $port = 9092;

    public static function setUpBeforeClass()
    {

    }

    public function setUp()
    {
        parent::setUp();
    }
}
