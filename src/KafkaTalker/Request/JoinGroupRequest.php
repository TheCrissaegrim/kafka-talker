<?php
namespace KafkaTalker\Request;

use KafkaTalker\Logger;
use KafkaTalker\Packer;

class JoinGroupRequest extends AbstractRequest
{
    const API_KEY = 11;

    public function send($groupId, $sessionTimeout, $memberId, $protocolType, $groupProtocols)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add GroupId
        $data .= Packer::packStringSignedInt16($groupId);

        // Add SessionTimeout
        $data .= Packer::packSignedInt32($sessionTimeout);

        // Add MemberId
        $data .= Packer::packStringSignedInt16($memberId);

        // Add ProtocolType
        $data .= Packer::packStringSignedInt16($protocolType);

        // Add GroupProtocol count
        $data .= Packer::packSignedInt32(count($groupProtocols));

        // Add GroupProtocols
        foreach ($groupProtocols as $protocolName => $protocolMetadata) {
            // Add ProtocolName
            $data .= Packer::packStringSignedInt16($protocolName);

            // Add ProtocolMetadata
            $data .= Packer::packStringSignedInt32($protocolMetadata);
        }

        // Concat data length (32 bits) and data
        $data = Packer::packStringSignedInt32($data);

        // Sent data
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

        // Read GenerationId
        $generationId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> GenerationId: %s', var_export($generationId, true));
        $cursor += 4;

        // Read GroupProtocol length
        $groupProtocolLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
        Logger::log('> GroupProtocol length: %s', var_export($groupProtocolLength, true));
        $cursor += 2;

        // Read GroupProtocol
        $groupProtocol = substr($response, $cursor, $groupProtocolLength);
        Logger::log('> GroupProtocol: %s', var_export($groupProtocol, true));
        $cursor += $groupProtocolLength;

        // Read LeaderId length
        $leaderIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
        Logger::log('> LeaderId length: %s', var_export($leaderIdLength, true));
        $cursor += 2;

        // Read LeaderId
        $leaderId = substr($response, $cursor, $leaderIdLength);
        Logger::log('> LeaderId: %s', var_export($leaderId, true));
        $cursor += $leaderIdLength;

        // Read MemberId length
        $memberIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
        Logger::log('> MemberId length: %s', var_export($memberIdLength, true));
        $cursor += 2;

        // Read MemberId
        $memberId = substr($response, $cursor, $memberIdLength);
        Logger::log('> MemberId: %s', var_export($memberIdLength, true));
        $cursor += $memberIdLength;

        // Read Member count
        $memberCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> Member count: %s', var_export($memberCount, true));
        $cursor += 4;

        // Read Members
        $members = [];
        for ($i = 1; $i <= $memberCount; $i++) {
            Logger::log('    > [Member #%d]', $i);

            // Read MemberId length
            $memberIdLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > MemberId length: %s', var_export($memberIdLength, true));
            $cursor += 2;

            // Read MemberId
            $memberId = substr($response, $cursor, $memberIdLength);
            Logger::log('        > MemberId: %s', var_export($memberIdLength, true));
            $cursor += $memberIdLength;

            // TODO: read MemberMetaData

            // Read MemberMetaData size
            $memberMetaDataSize = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            Logger::log('        > MemberMetaData size: %s', var_export($memberMetaDataSize, true));
            $cursor += 4;

            // Read MemberMetaData
            $memberMetaData = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            Logger::log('        > MemberMetaData: %s', var_export($memberMetaData, true));
            $cursor += $memberMetaDataSize;

            $members[] = [
                'MemberId'          => $memberId,
                'MemberMetaData'    => $memberMetaData,
            ];
        }

        return [
            'CorrelationId' => $correlationId,
            'ErrorCode'     => $errorCode,
            'GenerationId'  => $generationId,
            'GroupProtocol' => $groupProtocol,
            'LeaderId'      => $leaderId,
            'MemberId'      => $memberId,
            'Members'       => $members,
        ];
    }
}
