<?php
namespace KafkaTalker\Request;

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
        if ($this->debug) {
            printf("Response length: %s\n", var_export($responseLength, true));
        }

        // Response
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

        // Read Broker count
        $brokerCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> Broker count: %s\n", var_export($brokerCount, true));
        }
        $cursor += 4;

        // Read Brokers
        $brokers = [];
        for ($i = 1; $i <= $brokerCount; $i++) {
            if ($this->debug) {
                printf("    > [Broker #%d]\n", $i);
            }

            // Read NodeId
            $nodeId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            if ($this->debug) {
                printf("        > NodeId: %s\n", var_export($nodeId, true));
            }
            $cursor += 4;

            // Read Host length
            $hostLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > Host length: %s\n", var_export($hostLength, true));
            }
            $cursor += 2;

            // Read Host
            $host = substr($response, $cursor, $hostLength);
            if ($this->debug) {
                printf("        > Host: %s\n", var_export($host, true));
            }
            $cursor += $hostLength;

            // Read Port
            $port = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            if ($this->debug) {
                printf("        > Port: %s\n", var_export($port, true));
            }
            $cursor += 4;

            $brokers[] = [
                'NodeId'    => $nodeId,
                'Host'      => $host,
                'Port'      => $port,
            ];
        }

        // Read TopicMetadata count
        $topicMetadataCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> TopicMetadata count: %s\n", var_export($topicMetadataCount, true));
        }
        $cursor += 4;

        // Read TopicMetadata
        $topicMetadatas = [];
        for ($i = 1; $i <= $topicMetadataCount; $i++) {
            if ($this->debug) {
                printf("    > [TopicMetadata #%d]\n", $i);
            }

            // Read TopicErrorCode
            $topicErrorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > TopicErrorCode: %s\n", var_export($topicErrorCode, true));
            }
            $cursor += 2;

            // Read Topic length
            $topicLength = Packer::unpackSignedInt16(substr($response, $cursor, 2));
            if ($this->debug) {
                printf("        > Topic length: %s\n", var_export($topicLength, true));
            }
            $cursor += 2;

            // Read Topic
            $topic = substr($response, $cursor, $topicLength);
            if ($this->debug) {
                printf("        > Topic: %s\n", var_export($topic, true));
            }
            $cursor += $topicLength;

            // Read PartitionMetadata count
            $partitionMetadataCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            if ($this->debug) {
                printf("        > PartitionMetadata count: %s\n", var_export($partitionMetadataCount, true));
            }
            $cursor += 4;

            // Read PartitionMetadata
            $partitionMetadatas = [];
            for ($j = 1; $j <= $partitionMetadataCount; $j++) {
                if ($this->debug) {
                    printf("            > [PartitionMetadata #%d]\n", $j);
                }

                // Read PartitionErrorCode
                $partitionErrorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("                > PartitionErrorCode: %s\n", var_export($partitionErrorCode, true));
                }
                $cursor += 2;

                // Read PartitionId
                $partitionId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > PartitionId: %s\n", var_export($partitionId, true));
                }
                $cursor += 4;

                // Read Leader
                $leaderId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > LeaderId: %s\n", var_export($leaderId, true));
                }
                $cursor += 4;

                // Read Replica count
                $replicaCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > Replica count: %s\n", var_export($replicaCount, true));
                }
                $cursor += 4;

                // Read Replicas
                $replicas = [];
                for ($k = 1; $k <= $replicaCount; $k++) {
                    if ($this->debug) {
                        printf("                    > [Replica #%d]\n", $k);
                    }

                    // Read Replica
                    $replica = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    if ($this->debug) {
                        printf("                        > Replica: %s\n", var_export($replica, true));
                    }
                    $cursor += 4;

                    $replicas[] = $replica;
                }

                // Read Isr count
                $isrCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > Isr count: %s\n", var_export($isrCount, true));
                }
                $cursor += 4;

                // Read Isrs
                $isrs = [];
                for ($k = 1; $k <= $isrCount; $k++) {
                    if ($this->debug) {
                        printf("                    > [Isr #%d]\n", $k);
                    }

                    // Read Isr
                    $isr = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    if ($this->debug) {
                        printf("                        > Isr: %s\n", var_export($isr, true));
                    }
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
            'TopicMetadata' => $topicMetadatas,
        ];
    }
}
