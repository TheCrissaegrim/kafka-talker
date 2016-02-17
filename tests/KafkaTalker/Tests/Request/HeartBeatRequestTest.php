<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\HeartBeatRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class HeartBeatRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client();
        $client->setKafkaVersion('0.8.2.2');
        $client->connect($this->host, $this->port);

        $correlationId = mt_rand(-32768, 32767);

        $heartBeatRequest = new HeartBeatRequest($client);
        $heartBeatRequest->setCorrelationId($correlationId);
        $heartBeatRequest->send('GroupId1', 'GenerationId1', 'MemberId1');
        $response = $heartBeatRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
        $this->assertArrayHasKey('ErrorCode', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
