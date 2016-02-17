<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\DescribeGroupsRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class DescribeGroupRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port);
        $client->setKafkaVersion('0.8.2.2');
        $client->connect();

        $correlationId = mt_rand(-32768, 32767);

        $describeGroupsRequest = new DescribeGroupsRequest($client);
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
