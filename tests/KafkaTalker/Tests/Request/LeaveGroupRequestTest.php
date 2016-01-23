<?php
namespace KafkaTalker\Tests\Request;

use KafkaTalker\Client;
use KafkaTalker\Request\LeaveGroupRequest;
use KafkaTalker\Tests\KafkaTalkerTest;

class LeaveGroupRequestTest extends KafkaTalkerTest
{
    public function testReceive()
    {
        $client = new Client($this->host, $this->port, ['kafka_version' => '0.8.2.2']);

        $correlationId = mt_rand(-32768, 32767);

        $leaveGroupRequest = new LeaveGroupRequest($client);
        $leaveGroupRequest->setCorrelationId($correlationId);
        $leaveGroupRequest->send('GroupId1', 'MemberId1');
        $response = $leaveGroupRequest->receive();

        $this->assertInternalType('array', $response);
        $this->assertArrayHasKey('CorrelationId', $response);
        $this->assertSame($correlationId, $response['CorrelationId']);
        $this->assertArrayHasKey('ErrorCode', $response);
        $this->assertInternalType('int', $response['ErrorCode']);
    }
}
