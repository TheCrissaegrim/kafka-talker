<?php
namespace KafkaTalker\Request;

use KafkaTalker\Logger;
use KafkaTalker\Packer;

class SyncGroupRequest extends AbstractRequest
{
    const API_KEY = 14;

    public function send($groupId, $generationId, $memberId, $groupAssignments)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add GroupId
        $data .= Packer::packStringSignedInt16($groupId);

        // Add GenerationId
        $data .= Packer::packSignedInt32($generationId);

        // Add MemberId
        $data .= Packer::packStringSignedInt16($memberId);

        // Add GroupAssignment count
        $data .= Packer::packSignedInt32(count($groupAssignments));
        foreach ($groupAssignments as $groupAssignment) {
            // Add MemberId
            $data .= Packer::packStringSignedInt16($groupAssignment['MemberId']);

            // Add MemberAssignment
            $data .= Packer::packStringSignedInt32($groupAssignment['MemberAssignment']);
        }

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
        Logger::log('Reponse length: %s', var_export($responseLength, true));

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

        // Read MemberAssignment length
        $memberAssignmentLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> MemberAssignment length: %s', var_export($memberAssignmentLength, true));
        $cursor += 4;

        // Read MemberAssignment
        $memberAssignment = substr($response, $cursor, $memberAssignmentLength);
        Logger::log('> MemberAssignment: %s', var_export($memberAssignment, true));
        $cursor += $memberAssignmentLength;

        return [
            'CorrelationId'     => $correlationId,
            'ErrorCode'         => $errorCode,
            'MemberAssignment'  => $memberAssignment,
        ];
    }
}
