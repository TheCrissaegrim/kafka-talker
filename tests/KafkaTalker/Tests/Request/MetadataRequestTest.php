<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\MetadataRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class MetadataRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port, ['debug' => $this->debug, 'kafka_version' => '0.8.2.2']);

        $correlationId = mt_rand(-32768, 32767);

        $metadataRequest = new MetadataRequest($client, ['debug' => $this->debug]);
        $metadataRequest->setCorrelationId($correlationId);
        $metadataRequest->send([]);
        $response = $metadataRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }
}
