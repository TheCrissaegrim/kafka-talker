<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\ProduceRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class ProduceRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port);
        $client->setKafkaVersion('0.8.2.2');
        $client->connect();

        $correlationId = mt_rand(-32768, 32767);

        $produceRequest = new ProduceRequest($client);
        $produceRequest->setCorrelationId($correlationId);
        $produceRequest->send(1, 6000, ['kafka_talker_unit_tests_1' => [0 => ['one', 'two', 'three', 'four']]]);
        $response = $produceRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
