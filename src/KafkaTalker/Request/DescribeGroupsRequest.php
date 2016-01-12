<?php
namespace KafkaTalker\Request;

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

        // Read Group count
        $groupCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> Group count: %s\n", var_export($groupCount, true));
        }
        $cursor += 4;

        // Read Groups
        $groups = [];
        for ($i = 1; $i <= $groupCount; $i++) {
            if ($this->debug) {
                printf("    > [Group #%d]\n", $i);
            }

            // Read ErrorCode
            $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > ErrorCode: %s\n", var_export($errorCode, true));
            }
            $cursor += 2;

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

            // Read State length
            $stateLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > State length: %s\n", var_export($stateLength, true));
            }
            $cursor += 2;

            // Read State
            $state = substr($response, $cursor, $stateLength);
            if ($this->debug) {
                printf("        > State: %s\n", var_export($state, true));
            }
            $cursor += $stateLength;

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

            // Read Protocol length
            $protocolLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > Protocol length: %s\n", var_export($protocolLength, true));
            }
            $cursor += 2;

            // Read Protocol
            $protocol = substr($response, $cursor, $protocolLength);
            if ($this->debug) {
                printf("        > Protocol: %s\n", var_export($protocol, true));
            }
            $cursor += $protocolLength;

            // Read Member count
            $memberCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            if ($this->debug) {
                printf("        > Member count: %s\n", var_export($memberCount, true));
            }
            $cursor += 4;

            // Read Members
            $members = [];
            for ($j = 1; $j <= $memberCount; $j++) {
                if ($this->debug) {
                    printf("            > [Member #%d]\n", $j);
                }

                // Read MemberId length
                $memberIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("                > MemberId length: %s\n", var_export($memberIdLength, true));
                }
                $cursor += 2;

                // Read MemberId
                $memberId = substr($response, $cursor, $memberIdLength);
                if ($this->debug) {
                    printf("                > MemberId: %s\n", var_export($memberId, true));
                }
                $cursor += $memberIdLength;

                // Read ClientId length
                $clientIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("                > ClientId length: %s\n", var_export($clientIdLength, true));
                }
                $cursor += 2;

                // Read ClientId
                $clientId = substr($response, $cursor, $clientIdLength);
                if ($this->debug) {
                    printf("                > ClientId: %s\n", var_export($clientId, true));
                }
                $cursor += $clientIdLength;

                // Read ClientHost length
                $clientHostLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("                > ClientHost length: %s\n", var_export($clientHostLength, true));
                }
                $cursor += 2;

                // Read ClientHost
                $clientHost = substr($response, $cursor, $clientHostLength);
                if ($this->debug) {
                    printf("                > ClientHost: %s\n", var_export($clientHost, true));
                }
                $cursor += $clientHostLength;

                // Read MemberMetadata length
                $memberMetadataLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > MemberMetadata length: %s\n", var_export($memberMetadataLength, true));
                }
                $cursor += 4;

                // Read MemberMetadata
                $memberMetadata = substr($response, $cursor, $memberMetadataLength);
                if ($this->debug) {
                    printf("                > MemberMetadata: %s\n", var_export($memberMetadata, true));
                }
                $cursor += $memberMetadataLength;

                // Read MemberAssignment length
                $memberAssignmentLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > MemberAssignment length: %s\n", var_export($memberAssignmentLength, true));
                }
                $cursor += 4;

                // Read MemberAssignment
                $memberAssignment = substr($response, $cursor, $memberAssignmentLength);
                if ($this->debug) {
                    printf("                > MemberAssignment: %s\n", var_export($memberAssignment, true));
                }
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
