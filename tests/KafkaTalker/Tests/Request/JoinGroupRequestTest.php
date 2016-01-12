<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\JoinGroupRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class JoinGroupRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port, ['debug' => $this->debug, 'kafka_version' => '0.8.2.2']);

        $correlationId = mt_rand(-32768, 32767);

        $joinGroupRequest = new JoinGroupRequest($client, ['debug' => $this->debug]);
        $joinGroupRequest->setCorrelationId($correlationId);
        $joinGroupRequest->send('ConsumerGroup1', 6000, 'MemberId1', 'consumer', ['ProtocolName1' => 'ProtocolMetadata1']);
        $response = $joinGroupRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
