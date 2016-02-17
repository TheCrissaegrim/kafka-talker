<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\ListGroupsRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class ListGroupsRequestTest extends KafkaTalkerTest
{
    public function testOne()
    {
        $client = new Client();
        $client->setKafkaVersion('0.8.2.2');
        $client->connect($this->host, $this->port);

        $correlationId = mt_rand(-32768, 32767);

        $listGroupsRequest = new ListGroupsRequest($client);
        $listGroupsRequest->setCorrelationId($correlationId);
        $listGroupsRequest->send();
        $response = $listGroupsRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
