<?php
namespace KafkaTalker\Request;

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
        if ($this->debug) {
            printf("Response length: %s\n", var_export($responseLength, true));
        }

        // Read response
        $response = $this->client->read($responseLength);
        if ($this->debug) {
            printf("Response (packed): %s\n", var_export($response, true));
        }

        $cursor = 0;

        // Read CorrelationId
        $correlationId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> CorrelationId: %s\n", var_export($correlationId, true));
        }
        $cursor += 4;

        // Read ErrorCode
        $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
        if ($this->debug) {
            printf("> ErrorCode: %s\n", var_export($errorCode, true));
        }
        $cursor += 2;

        // Read Group count
        $groupCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> Group count: %s\n", var_export($groupCount, true));
        }
        $cursor += 4;

        $groups = [];

        // Read Groups
        for ($i = 1; $i <= $groupCount; $i++) {
            if ($this->debug) {
                printf("    > [Group #%d]\n", $i);
            }

            // Read GroupId length
            $groupIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > GroupId length: %s\n", var_export($groupIdLength, true));
            }
            $cursor += 2;

            // Read GroupId
            $groupId = substr($response, $cursor, $groupIdLength);
            if ($this->debug) {
                printf("        > GroupId: %s\n", var_export($groupId, true));
            }
            $cursor += $groupIdLength;

            // Read ProtocolType length
            $protocolTypeLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > ProtocolType length: %s\n", var_export($protocolTypeLength, true));
            }
            $cursor += 2;

            // Read ProtocolType
            $protocolType = substr($response, $cursor, $protocolTypeLength);
            if ($this->debug) {
                printf("        > ProtocolType: %s\n", var_export($protocolType, true));
            }
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
