<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\GroupCoordinatorRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class GroupCoordinatorRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client();
        $client->setKafkaVersion('0.8.2.2');
        $client->connect($this->host, $this->port);

        $correlationId = mt_rand(-32768, 32767);

        $groupCoordinatorRequest = new GroupCoordinatorRequest($client);
        $groupCoordinatorRequest->setCorrelationId($correlationId);
        $groupCoordinatorRequest->send('ConsumerGroup1');
        $response = $groupCoordinatorRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
