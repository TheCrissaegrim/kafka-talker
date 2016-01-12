<?php
namespace KafkaTalker\Request;

use KafkaTalker\Packer;

class OffsetFetchRequest extends AbstractRequest
{
    const API_KEY = 9;

    public function send($consumerGroup, $topics)
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
            foreach ($partitions as $partition) {
                // Add Partition
                $data .= Packer::packSignedInt32($partition);
            }
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

                // Read Offset
                $offset = Packer::unpackSignedInt64(substr($response, $cursor, 8));
                if ($this->debug) {
                    printf("            > Offset: %s\n", var_export($offset, true));
                }
                $cursor += 8;

                // Read Metadata length
                $metadataLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("            > Metadata length: %s\n", var_export($metadataLength, true));
                }
                $cursor += 2;

                // Read Metadata
                $metadata = substr($response, $cursor, $metadataLength);
                if ($this->debug) {
                    printf("            > Metadata: %s\n", var_export($metadata, true));
                }
                $cursor += $metadataLength;

                // Read ErrorCode
                $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("            > ErrorCode: %s\n", var_export($errorCode, true));
                }
                $cursor += 2;

                $partitions[] = [
                    'PartitionId'   => $partitionId,
                    'Offset'        => $offset,
                    'Metadata'      => $metadata,
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
