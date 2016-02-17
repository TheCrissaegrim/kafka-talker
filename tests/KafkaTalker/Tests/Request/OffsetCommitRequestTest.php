<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\OffsetCommitRequest;
use KafkaTalker\Tests\KafkaTalkerTest;


use KafkaTalker\Request\OffsetFetchRequest;

class OffsetCommitRequestTest extends KafkaTalkerTest
{
    public function testReceiveV0()
    {
        $client = new Client();
        $client->setKafkaVersion('0.8.2.2');
        $client->connect($this->host, $this->port);

        $correlationId = mt_rand(-32768, 32767);

        $offsetCommitRequest = new OffsetCommitRequest($client);
        $offsetCommitRequest->setCorrelationId($correlationId);
        $offsetCommitRequest->sendV0('toto', ['test1' => [0 => ['Offset' => 2, 'Metadata' => '']]]);



/*
        $offsetCommitRequest->sendV1(
            'ConsumerGroup1',
            'ConsumerGroupGenerationId1',
            'ConsumerId',
            [
                'kafka_talker_unit_tests_1' => [
                    0 => ['Offset' => 4, 'Timestamp' => ((int) date('U')) + 6000, 'Metadata' => 'toto']
                ]
            ]
        );
        $offsetCommitRequest->sendV2(
            'ConsumerGroup1',
            1,
            'ConsumerId',
            6000,
            [
                'kafka_talker_unit_tests_1' => [
                    0 => ['Offset' => 4, 'Metadata' => 'toto']
                ]
            ]
        );
*/


        $response = $offsetCommitRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
    }

/*
    public function testReceiveV1()
    {
        $client = new Client('localhost', 9092, ['debug' => $this->debug, 'kafka_version' => '0.8.3']);

        $offsetCommitRequest = new OffsetCommitRequest($client, ['debug' => $this->debug]);
        $offsetCommitRequest->sendV1(
            'ConsumerGroup1',
            'ConsumerGroupGenerationId1',
            'ConsumerId',
            [
                'kafka_talker_unit_tests_1' => [
                    0 => ['Offset' => 4, 'Timestamp' => ((int) date('U')) + 6000, 'Metadata' => 'toto']
                ]
            ]
        );
        $response = $offsetCommitRequest->receive();
        $this->assertInternalType('array', $response);
    }

    public function testReceiveV2()
    {
        $client = new Client('localhost', 9092, ['debug' => $this->debug, 'kafka_version' => '0.8.3']);

        $offsetCommitRequest = new OffsetCommitRequest($client, ['debug' => $this->debug]);
        $offsetCommitRequest->sendV2(
            'ConsumerGroup1',
            1,
            'ConsumerId',
            6000,
            [
                'kafka_talker_unit_tests_1' => [
                    0 => ['Offset' => 4, 'Metadata' => 'toto']
                ]
            ]
        );
        $response = $offsetCommitRequest->receive();
        $this->assertInternalType('array', $response);
    }
*/
}
