<?php
namespace KafkaTalker\Request;

use KafkaTalker\Packer;

class GroupCoordinatorRequest extends AbstractRequest
{
    const API_KEY = 10;

    public function send($groupId)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add GroupId
        $data .= Packer::packStringSignedInt16($groupId);

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
        $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> ErrorCode: %s\n", var_export($errorCode, true));
        }
        $cursor += 2;

        // Read CoordinatorId
        $coordinatorId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> CoordinatorId: %s\n", var_export($coordinatorId, true));
        }
        $cursor += 4;

        // Read CoordinatorHost length
        $coordinatorHostLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
        if ($this->debug) {
            printf("> CoordinatorHost length: %s\n", var_export($coordinatorHostLength, true));
        }
        $cursor += 2;

        // Read CoordinatorHost
        $coordinatorHost = substr($response, $cursor, $coordinatorHostLength);
        if ($this->debug) {
            printf("> CoordinatorHost: %s\n", var_export($coordinatorHost, true));
        }
        $cursor += $coordinatorHostLength;

        // Read CoordinatorPort
        $coordinatorPort = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> CoordinatorPort: %s\n", var_export($coordinatorPort, true));
        }
        $cursor += 4;

        return [
            'CorrelationId'     => $correlationId,
            'ErrorCode'         => $errorCode,
            'CoordinatorId'     => $coordinatorId,
            'CoordinatorHost'   => $coordinatorHost,
            'CoordinatorPort'   => $coordinatorPort,
        ];
    }
}
