<?php
namespace KafkaTalker\Request;

use KafkaTalker\Logger;
use KafkaTalker\Packer;

class MetadataRequest extends AbstractRequest
{
    const API_KEY = 3;

    public function send($topics)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add Topic count
        $data .= Packer::packSignedInt32(count($topics));

        // Add Topics
        foreach ($topics as $topic) {
            // Add Topic
            $data .= Packer::packStringSignedInt16($topic);
        }

        // Concat data length (32 bits) and data
        $data = Packer::packStringSignedInt32($data);

        // Send the message
        return $this->client->write($data);
    }

    public function receive()
    {
        // Read response length
        $responseLength = $this->client->read(4);
        $responseLength = Packer::unpackSignedInt32($responseLength);
        Logger::log('Response length: %s', var_export($responseLength, true));

        // Response
        $response = $this->client->read($responseLength);
        Logger::log('Response (packed): %s', var_export($response, true));

        $cursor = 0;

        // Read CorrelationId
        $correlationId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> CorrelationId: %s', var_export($correlationId, true));
        $cursor += 4;

        // Read Broker count
        $brokerCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> Broker count: %s', var_export($brokerCount, true));
        $cursor += 4;

        // Read Brokers
        $brokers = [];
        for ($i = 1; $i <= $brokerCount; $i++) {
            Logger::log('    > [Broker #%d]', $i);

            // Read NodeId
            $nodeId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            Logger::log('        > NodeId: %s', var_export($nodeId, true));
            $cursor += 4;

            // Read Host length
            $hostLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > Host length: %s', var_export($hostLength, true));
            $cursor += 2;

            // Read Host
            $host = substr($response, $cursor, $hostLength);
            Logger::log('        > Host: %s', var_export($host, true));
            $cursor += $hostLength;

            // Read Port
            $port = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            Logger::log('        > Port: %s', var_export($port, true));
            $cursor += 4;

            $brokers[] = [
                'NodeId'    => $nodeId,
                'Host'      => $host,
                'Port'      => $port,
            ];
        }

        // Read TopicMetadata count
        $topicMetadataCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> TopicMetadata count: %s', var_export($topicMetadataCount, true));
        $cursor += 4;

        // Read TopicMetadata
        $topicMetadatas = [];
        for ($i = 1; $i <= $topicMetadataCount; $i++) {
            Logger::log('    > [TopicMetadata #%d]', $i);

            // Read TopicErrorCode
            $topicErrorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > TopicErrorCode: %s', var_export($topicErrorCode, true));
            $cursor += 2;

            // Read Topic length
            $topicLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('        > Topic length: %s', var_export($topicLength, true));
            $cursor += 2;

            // Read Topic
            $topic = substr($response, $cursor, $topicLength);
            Logger::log('        > Topic: %s', var_export($topic, true));
            $cursor += $topicLength;

            // Read PartitionMetadata count
            $partitionMetadataCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            Logger::log('        > PartitionMetadata count: %s', var_export($partitionMetadataCount, true));
            $cursor += 4;

            // Read PartitionMetadata
            $partitionMetadatas = [];
            for ($j = 1; $j <= $partitionMetadataCount; $j++) {
                Logger::log('            > [PartitionMetadata #%d]', $j);

                // Read PartitionErrorCode
                $partitionErrorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                Logger::log('                > PartitionErrorCode: %s', var_export($partitionErrorCode, true));
                $cursor += 2;

                // Read PartitionId
                $partitionId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                Logger::log('                > PartitionId: %s', var_export($partitionId, true));
                $cursor += 4;

                // Read Leader
                $leaderId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                Logger::log('                > LeaderId: %s', var_export($leaderId, true));
                $cursor += 4;

                // Read Replica count
                $replicaCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                Logger::log('                > Replica count: %s', var_export($replicaCount, true));
                $cursor += 4;

                // Read Replicas
                $replicas = [];
                for ($k = 1; $k <= $replicaCount; $k++) {
                    Logger::log('                    > [Replica #%d]', $k);

                    // Read Replica
                    $replica = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    Logger::log('                        > Replica: %s', var_export($replica, true));
                    $cursor += 4;

                    $replicas[] = $replica;
                }

                // Read Isr count
                $isrCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                Logger::log('                > Isr count: %s', var_export($isrCount, true));
                $cursor += 4;

                // Read Isrs
                $isrs = [];
                for ($k = 1; $k <= $isrCount; $k++) {
                    Logger::log('                    > [Isr #%d]', $k);

                    // Read Isr
                    $isr = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    Logger::log('                        > Isr: %s', var_export($isr, true));
                    $cursor += 4;

                    $isrs[] = $isr;
                }

                $partitionMetadatas[] = [
                    'ErrorCode'     => $partitionErrorCode,
                    'PartitionId'   => $partitionId,
                    'LeaderId'      => $leaderId,
                    'Replica'       => $replicas,
                    'Isr'           => $isrs,
                ];
            }

            $topicMetadatas[] = [
                'ErrorCode'         => $topicErrorCode,
                'Topic'             => $topic,
                'PartitionMetadata' => $partitionMetadatas,
            ];
        }

        return [
            'CorrelationId' => $correlationId,
            'Brokers' => $brokers,
            'TopicMetadata' => $topicMetadatas,
        ];
    }
}
