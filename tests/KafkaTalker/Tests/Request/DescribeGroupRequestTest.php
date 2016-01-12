<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\DescribeGroupsRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class DescribeGroupRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port, ['debug' => $this->debug, 'kafka_version' => '0.8.2.2']);

        $correlationId = mt_rand(-32768, 32767);

        $describeGroupsRequest = new DescribeGroupsRequest($client, ['debug' => $this->debug]);
        $describeGroupsRequest->setCorrelationId($correlationId);
        $describeGroupsRequest->send(['GroupId1', 'GroupId2']);
        $response = $describeGroupsRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
        $this->assertArrayHasKey('Groups', $response);
        $this->assertInternalType('array', $response['Groups']);
    }
}
