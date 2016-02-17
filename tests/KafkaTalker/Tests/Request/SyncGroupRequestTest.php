<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\SyncGroupRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class SyncGroupRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port);
        $client->setKafkaVersion('0.8.2.2');
        $client->connect();

        $correlationId = mt_rand(-32768, 32767);

        $syncGroupRequest = new SyncGroupRequest($client);
        $syncGroupRequest->setCorrelationId($correlationId);
        $syncGroupRequest->send(
            'GroupId1',
            'GenerationId1',
            'MemberId1',
            [
                [
                    'MemberId' => 'MemberId1',
                    'MemberAssignment' => 'MemberAssignment1',
                ]
            ]
        );
        $response = $syncGroupRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
