<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\FetchRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class FetchRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port, ['debug' => $this->debug, 'kafka_version' => '0.8.2.2']);

        $correlationId = mt_rand(-32768, 32767);

        $fetchRequest = new FetchRequest($client, ['debug' => $this->debug]);
        $fetchRequest->setCorrelationId($correlationId);
        $fetchRequest->send(-1, 100, 1024, ['kafka_talker_unit_tests_1' => [0 => ['offset' => 8589934593, 'max_bytes' => 100 * 1024 * 1024]]]);
        $response = $fetchRequest->receive();

        //$this->assertInternalType('array', $response);
        //$this->assertArrayHasKey('CorrelationId', $response);
        //$this->assertSame($correlationId, $response['CorrelationId']);
    }
}
