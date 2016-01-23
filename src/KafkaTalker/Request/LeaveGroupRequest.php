<?php
namespace KafkaTalker\Request;

use KafkaTalker\Logger;
use KafkaTalker\Packer;

class LeaveGroupRequest extends AbstractRequest
{
    const API_KEY = 13;

    public function send($groupId, $memberId)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add GroupId
        $data .= Packer::packStringSignedInt16($groupId);

        // Add MemberId
        $data .= Packer::packStringSignedInt16($memberId);

        // Concat data length (32 bits) and data
        $data = Packer::packStringSignedInt32($data);

        // Send data
        return $this->client->write($data);
    }

    public function receive()
    {
        // Read response length
        $responseLength = $this->client->read(4);
        $responseLength = Packer::unpackSignedInt32($responseLength);
        Logger::log('Response length: %s', var_export($responseLength, true));

        // Read response
        $response = $this->client->read($responseLength);
        Logger::log('Response (packed): %s', var_export($response, true));

        $cursor = 0;

        // Read CorrelationId
        $correlationId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('CorrelationId: %s', var_export($correlationId, true));
        $cursor += 4;

        // Read ErrorCode
        $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
        Logger::log('ErrorCode: %s', var_export($errorCode, true));
        $cursor += 2;

        return [
            'CorrelationId' => $correlationId,
            'ErrorCode'     => $errorCode,
        ];
    }
}
