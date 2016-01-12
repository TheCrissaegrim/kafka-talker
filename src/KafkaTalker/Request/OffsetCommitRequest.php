<?php
namespace KafkaTalker\Request;

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
        if ($this->debug) {
            printf("> Packing ConsumerGroup length: %s\n", var_export(strlen($consumerGroup), true));
            printf("> Packing ConsumerGroup: %s\n", var_export($consumerGroup, true));
        }
        $data .= Packer::packSignedInt16(strlen($consumerGroup)) . $consumerGroup;

        // Add ConsumerGroupGenerationId
        if ($this->debug) {
            printf("> Packing ConsumerGroupGenerationId: %s\n", var_export($consumerGroupGenerationId, true));
        }
        $data .= Packer::packSignedInt32($consumerGroupGenerationId);

        // Add ConsumerId
        if ($this->debug) {
            printf("> Packing ConsumerId length: %s\n", var_export(strlen($consumerId), true));
            printf("> Packing ConsumerId: %s\n", var_export($consumerId, true));
        }
        $data .= Packer::packSignedInt16(strlen($consumerId)) . $consumerId;

        // Add RetentionTime
        if ($this->debug) {
            printf("> Packing RetentionTime: %s\n", var_export($retentionTime, true));
        }
        $data .= Packer::packSignedInt64($retentionTime);

        // Add Topic count
        if ($this->debug) {
            printf("> Packing Topic count: %s\n", var_export(count($topics), true));
        }
        $data .= Packer::packSignedInt32(count($topics));

        // Add Topics
        foreach ($topics as $topic => $partitions) {
            // Add Topic
            if ($this->debug) {
                printf("> Packing Topic length: %s\n", var_export(strlen($topic), true));
                printf("> Packing Topic: %s\n", var_export($topic, true));
            }
            $data .= Packer::packSignedInt16(strlen($topic)) . $topic;

            // Add Partition count
            if ($this->debug) {
                printf("> Packing Partition count: %s\n", var_export(count($partitions), true));
            }
            $data .= Packer::packSignedInt32(count($partitions));

            // Add Partitions
            foreach ($partitions as $partition => $partitionParams) {
                // Add Partition
                if ($this->debug) {
                    printf("> Packing Partition: %s\n", var_export(count($partition), true));
                }
                $data .= Packer::packSignedInt32($partition);

                // Add Offset
                if ($this->debug) {
                    printf("> Packing Offset: %s\n", var_export($partitionParams['Offset'], true));
                }
                $data .= Packer::packSignedInt64($partitionParams['Offset']);

                // Add Metadata
                if ($this->debug) {
                    printf("> Packing Metadata length: %s\n", var_export(strlen($partitionParams['Metadata']), true));
                    printf("> Packing Metadata: %s\n", var_export($partitionParams['Metadata'], true));
                }
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

        // Read Topic count
        $topicCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> Topic count: %s\n", var_export($topicCount, true));
        }
        $cursor += 4;

        // Read Topics
        $topics = [];
        for ($i = 1; $i <= $topicCount; $i++) {
            // Read Topic length
            $topicLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("    > Topic length: %s\n", var_export($topicLength, true));
            }
            $cursor += 2;

            // Read Topic
            $topic = substr($response, $cursor, $topicLength);
            if ($this->debug) {
                printf("    > Topic: %s\n", var_export($topic, true));
            }
            $cursor += $topicLength;

            // Read Parition count
            $partitionCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            if ($this->debug) {
                printf("    > Partition count: %s\n", var_export($partitionCount, true));
            }
            $cursor += 4;

            // Read Partitions
            $partitions = [];
            for ($j = 1; $j <= $partitionCount; $j++) {
                if ($this->debug) {
                    printf("        > [Partition #%d]\n", $j);
                }

                // Read Partition
                $partitionId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("            > PartitionId: %s\n", var_export($partitionId, true));
                }
                $cursor += 4;

                // Read ErrorCode
                $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("            > ErrorCode: %s\n", var_export($errorCode, true));
                }
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
