<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\OffsetRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class OffsetRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port, ['kafka_version' => '0.8.2.2']);

        $correlationId = mt_rand(-32768, 32767);

        $offsetRequest = new OffsetRequest($client);
        $offsetRequest->setCorrelationId($correlationId);
        $offsetRequest->send(-1, ['kafka_talker_unit_tests_1' => [0 => ['Time' => -1, 'MaxNumberOfOffsets' => 100000]]]);
        $response = $offsetRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
