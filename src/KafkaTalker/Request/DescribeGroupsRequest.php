<?php
namespace KafkaTalker\Request;

use KafkaTalker\Logger;
use KafkaTalker\Packer;

class DescribeGroupsRequest extends AbstractRequest
{
    const API_KEY = 15;

    public function send($groupIds)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add GroupIds
        $data .= Packer::packSignedInt32(count($groupIds));
        foreach ($groupIds as $groupId) {
            // Add GroupId
            $data .= Packer::packStringSignedInt16($groupId);
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
        Logger::log('Response length: %s', var_export($responseLength, true));

        // Read response
        $response = $this->client->read($responseLength);
        Logger::log('Response (packed): %s', var_export($response, true));

        $cursor = 0;

        // Read CorrelationId
        $correlationId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> CorrelationId: %s', var_export($correlationId, true));
        $cursor += 4;

        // Read Group count
        $groupCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> Group count: %s', var_export($groupCount, true));
        $cursor += 4;

        // Read Groups
        $groups = [];
        for ($i = 1; $i <= $groupCount; $i++) {
            Logger::log('    > [Group #%d]', $i);

            // Read ErrorCode
            $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > ErrorCode: %s', var_export($errorCode, true));
            $cursor += 2;

            // Read GroupId length
            $groupIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > GroupId length: %s', var_export($groupIdLength, true));
            $cursor += 2;

            // Read GroupId
            $groupId = substr($response, $cursor, $groupIdLength);
            Logger::log('        > GroupId: %s', var_export($groupId, true));
            $cursor += $groupIdLength;

            // Read State length
            $stateLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > State length: %s', var_export($stateLength, true));
            $cursor += 2;

            // Read State
            $state = substr($response, $cursor, $stateLength);
            Logger::log('        > State: %s', var_export($state, true));
            $cursor += $stateLength;

            // Read ProtocolType length
            $protocolTypeLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > ProtocolType length: %s', var_export($protocolTypeLength, true));
            $cursor += 2;

            // Read ProtocolType
            $protocolType = substr($response, $cursor, $protocolTypeLength);
            Logger::log('        > ProtocolType: %s', var_export($protocolType, true));
            $cursor += $protocolTypeLength;

            // Read Protocol length
            $protocolLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > Protocol length: %s', var_export($protocolLength, true));
            $cursor += 2;

            // Read Protocol
            $protocol = substr($response, $cursor, $protocolLength);
            Logger::log('        > Protocol: %s', var_export($protocol, true));
            $cursor += $protocolLength;

            // Read Member count
            $memberCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            Logger::log('        > Member count: %s', var_export($memberCount, true));
            $cursor += 4;

            // Read Members
            $members = [];
            for ($j = 1; $j <= $memberCount; $j++) {
                Logger::log('            > [Member #%d]', $j);

                // Read MemberId length
                $memberIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                Logger::log('                > MemberId length: %s', var_export($memberIdLength, true));
                $cursor += 2;

                // Read MemberId
                $memberId = substr($response, $cursor, $memberIdLength);
                Logger::log('                > MemberId: %s', var_export($memberId, true));
                $cursor += $memberIdLength;

                // Read ClientId length
                $clientIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                Logger::log('                > ClientId length: %s', var_export($clientIdLength, true));
                $cursor += 2;

                // Read ClientId
                $clientId = substr($response, $cursor, $clientIdLength);
                Logger::log('                > ClientId: %s', var_export($clientId, true));
                $cursor += $clientIdLength;

                // Read ClientHost length
                $clientHostLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                Logger::log('                > ClientHost length: %s', var_export($clientHostLength, true));
                $cursor += 2;

                // Read ClientHost
                $clientHost = substr($response, $cursor, $clientHostLength);
                Logger::log('                > ClientHost: %s', var_export($clientHost, true));
                $cursor += $clientHostLength;

                // Read MemberMetadata length
                $memberMetadataLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                Logger::log('                > MemberMetadata length: %s', var_export($memberMetadataLength, true));
                $cursor += 4;

                // Read MemberMetadata
                $memberMetadata = substr($response, $cursor, $memberMetadataLength);
                Logger::log('                > MemberMetadata: %s', var_export($memberMetadata, true));
                $cursor += $memberMetadataLength;

                // Read MemberAssignment length
                $memberAssignmentLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                Logger::log('                > MemberAssignment length: %s', var_export($memberAssignmentLength, true));
                $cursor += 4;

                // Read MemberAssignment
                $memberAssignment = substr($response, $cursor, $memberAssignmentLength);
                Logger::log('                > MemberAssignment: %s', var_export($memberAssignment, true));
                $cursor += $memberAssignmentLength;

                $members[] = [
                    'MemberId'          => $memberId,
                    'ClientId'          => $clientId,
                    'ClientHost'        => $clientHost,
                    'MemberMetadata'    => $memberMetadata,
                    'MemberAssignment'  => $memberAssignment,
                ];
            }

            $groups[] = [
                'ErrorCode'     => $errorCode,
                'GroupId'       => $groupId,
                'State'         => $state,
                'ProtocolType'  => $protocolType,
                'Protocol'      => $protocol,
                'Members'       => $members,
            ];
        }

        return [
            'CorrelationId' => $correlationId,
            'Groups'        => $groups,
        ];
    }
}
