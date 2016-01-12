<?php
namespace KafkaTalker\Request;

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
        if ($this->debug) {
            printf("Reponse length: %s\n", var_export($responseLength, true));
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

        // Read MemberAssignment length
        $memberAssignmentLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> MemberAssignment length: %s\n", var_export($memberAssignmentLength, true));
        }
        $cursor += 4;

        // Read MemberAssignment
        $memberAssignment = substr($response, $cursor, $memberAssignmentLength);
        if ($this->debug) {
            printf("> MemberAssignment: %s\n", var_export($memberAssignment, true));
        }
        $cursor += $memberAssignmentLength;

        return [
            'CorrelationId'     => $correlationId,
            'ErrorCode'         => $errorCode,
            'MemberAssignment'  => $memberAssignment,
        ];
    }
}
