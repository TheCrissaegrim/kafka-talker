<?php
namespace KafkaTalker\Request;

use KafkaTalker\Logger;
use KafkaTalker\Packer;

class OffsetCommitRequest extends AbstractRequest
{
    const API_KEY = 8;

    public function sendV0($consumerGroup, $topics)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add ConsumerGroup
        $data .= Packer::packStringSignedInt16($consumerGroup);

        // Add Topic count
        $data .= Packer::packSignedInt32(count($topics));

        // Add Topics
        foreach ($topics as $topic => $partitions) {
            // Add Topic
            $data .= Packer::packStringSignedInt16($topic);

            // Add Partition count
            $data .= Packer::packSignedInt32(count($partitions));

            // Add Partitions
            foreach ($partitions as $partition => $partitionParams) {
                // Add Partition
                $data .= Packer::packSignedInt32($partition);

                // Add Offset
                $data .= Packer::packSignedInt64($partitionParams['Offset']);

                // Add Metadata
                //$data .= Packer::packStringSignedInt16($partitionParams['Metadata']);
                $data .= Packer::packSignedInt16(-1);
            }
        }

        // Concat data length (32 bits) and data
        $data = Packer::packStringSignedInt32($data);

        // Send data
        return $this->client->write($data);
    }

    public function sendV1($consumerGroup, $consumerGroupGenerationId, $consumerId, $topics)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader(['api_version' => 1]);

        // Add ConsumerGroup
        $data .= Packer::packSignedInt16(strlen($consumerGroup)) . $consumerGroup;

        // Add ConsumerGroupGenerationId
        $data .= Packer::packSignedInt32($consumerGroupGenerationId);

        // Add ConsumerId
        $data .= Packer::packSignedInt16(strlen($consumerId)) . $consumerId;

        // Add Topic count
        $data .= Packer::packSignedInt32(count($topics));

        // Add Topics
        foreach ($topics as $topic => $partitions) {
            // Add Topic
            $data .= Packer::packSignedInt16(strlen($topic)) . $topic;

            // Add Partition count
            $data .= Packer::packSignedInt32(count($partitions));

            // Add Partitions
            foreach ($partitions as $partition => $partitionParams) {
                // Add Partition
                $data .= Packer::packSignedInt32($partition);

                // Add Offset
                $data .= Packer::packSignedInt64($partitionParams['Offset']);

                // Add Timestamp
                $data .= Packer::packSignedInt64($partitionParams['Timestamp']);

                // Add Metadata
                $data .= Packer::packSignedInt16(strlen($partitionParams['Metadata'])) . $partitionParams['Metadata'];
            }
        }

        // Concat data length (32 bits) and data
        $data = Packer::packSignedInt32(strlen($data)) . $data;

        // Send data
        return $this->client->write($data);
    }

    public function sendV2($consumerGroup, $consumerGroupGenerationId, $consumerId, $retentionTime, $topics)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader(['api_version' => 2]);

        // Add ConsumerGroup
        Logger::log('> Packing ConsumerGroup: %s', var_export($consumerGroup, true));
        $data .= Packer::packSignedInt16(strlen($consumerGroup)) . $consumerGroup;

        // Add ConsumerGroupGenerationId
        Logger::log('> Packing ConsumerGroupGenerationId: %s', var_export($consumerGroupGenerationId, true));
        $data .= Packer::packSignedInt32($consumerGroupGenerationId);

        // Add ConsumerId
        Logger::log('> Packing ConsumerId: %s', var_export($consumerId, true));
        $data .= Packer::packSignedInt16(strlen($consumerId)) . $consumerId;

        // Add RetentionTime
        Logger::log('> Packing RetentionTime: %s', var_export($retentionTime, true));
        $data .= Packer::packSignedInt64($retentionTime);

        // Add Topic count
        Logger::log('> Packing Topic count: %s', var_export(count($topics), true));
        $data .= Packer::packSignedInt32(count($topics));

        // Add Topics
        foreach ($topics as $topic => $partitions) {
            // Add Topic
            Logger::log('> Packing Topic: %s', var_export($topic, true));
            $data .= Packer::packSignedInt16(strlen($topic)) . $topic;

            // Add Partition count
            Logger::log('> Packing Partition count: %s', var_export(count($partitions), true));
            $data .= Packer::packSignedInt32(count($partitions));

            // Add Partitions
            foreach ($partitions as $partition => $partitionParams) {
                // Add Partition
                Logger::log('> Packing Partition: %s', var_export(count($partition), true));
                $data .= Packer::packSignedInt32($partition);

                // Add Offset
                Logger::log('> Packing Offset: %s', var_export($partitionParams['Offset'], true));
                $data .= Packer::packSignedInt64($partitionParams['Offset']);

                // Add Metadata
                Logger::log('> Packing Metadata: %s', var_export($partitionParams['Metadata'], true));
                $data .= Packer::packSignedInt16(strlen($partitionParams['Metadata'])) . $partitionParams['Metadata'];
            }
        }

        // Concat data length (32 bits) and data
        $data = Packer::packSignedInt32(strlen($data)) . $data;

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

        // Read Topic count
        $topicCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        Logger::log('> Topic count: %s', var_export($topicCount, true));
        $cursor += 4;

        // Read Topics
        $topics = [];
        for ($i = 1; $i <= $topicCount; $i++) {
            // Read Topic length
            $topicLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            Logger::log('    > Topic length: %s', var_export($topicLength, true));
            $cursor += 2;

            // Read Topic
            $topic = substr($response, $cursor, $topicLength);
            Logger::log('    > Topic: %s', var_export($topic, true));
            $cursor += $topicLength;

            // Read Parition count
            $partitionCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            Logger::log('    > Partition count: %s', var_export($partitionCount, true));
            $cursor += 4;

            // Read Partitions
            $partitions = [];
            for ($j = 1; $j <= $partitionCount; $j++) {
                Logger::log('        > [Partition #%d]', $j);

                // Read Partition
                $partitionId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                Logger::log('            > PartitionId: %s', var_export($partitionId, true));
                $cursor += 4;

                // Read ErrorCode
                $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                Logger::log('            > ErrorCode: %s', var_export($errorCode, true));
                $cursor += 2;

                $partitions[] = [
                    'PartitionId'   => $partitionId,
                    'ErrorCode'     => $errorCode,
                ];
            }

            $topics[] = [
                'Topic'         => $topic,
                'Partitions'    => $partitions,
            ];
        }

        return [
            'CorrelationId' => $correlationId,
            'Topics'        => $topics,
        ];
    }
}
