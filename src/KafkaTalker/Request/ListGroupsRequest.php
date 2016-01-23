<?php
namespace KafkaTalker\Request;

use KafkaTalker\Logger;
use KafkaTalker\Packer;

class ListGroupsRequest extends AbstractRequest
{
    const API_KEY = 16;

    public function send()
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

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
        Logger::log('> CorrelationId: %s', var_export($correlationId, true));
        $cursor += 4;

        // Read ErrorCode
        $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
        Logger::log('> ErrorCode: %s', var_export($errorCode, true));
        $cursor += 2;

        // Read Group count
        $groupCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> Group count: %s', var_export($groupCount, true));
        $cursor += 4;

        $groups = [];

        // Read Groups
        for ($i = 1; $i <= $groupCount; $i++) {
            Logger::log('    > [Group #%d]', $i);

            // Read GroupId length
            $groupIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > GroupId length: %s', var_export($groupIdLength, true));
            $cursor += 2;

            // Read GroupId
            $groupId = substr($response, $cursor, $groupIdLength);
            Logger::log('        > GroupId: %s', var_export($groupId, true));
            $cursor += $groupIdLength;

            // Read ProtocolType length
            $protocolTypeLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > ProtocolType length: %s', var_export($protocolTypeLength, true));
            $cursor += 2;

            // Read ProtocolType
            $protocolType = substr($response, $cursor, $protocolTypeLength);
            Logger::log('        > ProtocolType: %s', var_export($protocolType, true));
            $cursor += $protocolTypeLength;

            $groups[] = [
                'GroupId'       => $groupId,
                'ProtocolType'  => $protocolType,
            ];
        }

        return [
            'CorrelationId' => $correlationId,
            'ErrorCode'     => $errorCode,
            'Groups'        => $groups,
        ];
    }
}
