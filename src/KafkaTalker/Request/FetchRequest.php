<?php
namespace KafkaTalker\Request;

use KafkaTalker\Packer;

class FetchRequest extends AbstractRequest
{
    const API_KEY = 1;

    public function send($replicaId = -1, $maxWaitTime = 100, $minBytes = 1024, $topics)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add ReplicaId
        $data .= Packer::packSignedInt32($replicaId);

        // Add MaxWaitTime
        $data .= Packer::packSignedInt32($maxWaitTime);

        // Add MinBytes
        $data .= Packer::packSignedInt32($minBytes);

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

                // Add Partition offset
                $data .= Packer::packSignedInt64($partitionParams['offset']);

                // Add PartitionMaxBytes
                $data .= Packer::packSignedInt32($partitionParams['max_bytes']);
            }
        }

        // Concat data length (32 bits) and data
        $data = Packer::packStringSignedInt32($data);

        // Send data
        return $this->client->write($data);
    }

    public function receive($yield = false)
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

        // Read Topics count
        $topicCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
        if ($this->debug) {
            printf("> Topic count: %s\n", var_export($topicCount, true));
        }
        $cursor += 4;

        // Read Topics
        $topics = [];
        for ($i = 1; $i <= $topicCount; $i++) {
            if ($this->debug) {
                printf("    > [Topic #%d]\n", $i);
            }

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

            // Read Partition count
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
                    printf("                > ErrorCode: %s\n", var_export($errorCode, true));
                }
                $cursor += 2;

                // Read HighwaterMarkOffset
                $highwaterMarkOffset = Packer::unpackSignedInt64(substr($response, $cursor, 8));
                if ($this->debug) {
                    printf("                > HighwaterMarkOffset: %s\n", var_export($highwaterMarkOffset, true));
                }
                $cursor += 8;

                // Read MessageSet length
                $messageSetLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > MessageSet length: %s\n", var_export($messageSetLength, true));
                }

                $cursor += 4;

                $read = 0;
                $numMessages = 0;

                $messageSet = [];
                while ($read !== $messageSetLength) {
                    if ($this->debug) {
                        printf("                    > [Message #%d]\n", $numMessages);
                    }

                    // Read Offset
                    $offset = Packer::unpackSignedInt64(substr($response, $cursor, 8));
                    if ($this->debug) {
                        printf("                        > Offset: %s\n", var_export($offset, true));
                    }
                    $cursor += 8;
                    $read += 8;

                    // Read Message size
                    $messageSize = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    if ($this->debug) {
                        printf("                        > Message size: %s\n", var_export($messageSize, true));
                    }
                    $cursor += 4;
                    $read += 4;

                    // Read CRC
                    $crc = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    if ($this->debug) {
                        printf("                        > CRC: %s\n", var_export($crc, true));
                    }
                    $cursor += 4;
                    $read += 4;

                    // Read MagicByte
                    $magicByte = Packer::unpackSignedInt8(substr($response, $cursor, 1));
                    if ($this->debug) {
                        printf("                        > MagicByte: %s\n", var_export($magicByte, true));
                    }
                    $cursor += 1;
                    $read += 1;

                    // Read Attributes
                    $attributes = Packer::unpackSignedInt8(substr($response, $cursor, 1));
                    if ($this->debug) {
                        printf("                        > Attributes: %s\n", var_export($attributes, true));
                    }
                    $cursor += 1;
                    $read += 1;

                    // Read Key size
                    $keySize = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    if ($this->debug) {
                        printf("                        > Key size: %s\n", var_export($keySize, true));
                    }
                    $cursor += 4;
                    $read += 4;

                    // Read Key
                    if ($keySize !== -1) {
                        $key = substr($response, $cursor, $keySize);
                        $cursor += $keySize;
                        $read += $keySize;
                    } else {
                        printf("HAAAAAAAAAAAAAAA\n");
                        $key = null;
                    }

                    // Read Value length
                    $valueLength = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                    if ($this->debug) {
                        printf("                        > Value length: %s\n", var_export($valueLength, true));
                    }
                    $cursor += 4;
                    $read += 4;

                    // Read Value
                    $value = substr($response, $cursor, $valueLength);
                    if ($this->debug) {
                        printf("                        > Value: %s\n", var_export($value, true));
                    }
                    $cursor += $valueLength;
                    $read += $valueLength;

                    $numMessages++;

                    if ($yield) {
                        yield [
                            'Offset'        => $offset,
                            'CRC'           => $crc,
                            'MagicByte'     => $magicByte,
                            'Attributes'    => $attributes,
                            'Key'           => $key,
                            'Value'         => $value,
                        ];
                    }

                    $messageSet[] = [
                        'Offset'        => $offset,
                        'CRC'           => $crc,
                        'MagicByte'     => $magicByte,
                        'Attributes'    => $attributes,
                        'Key'           => $key,
                        'Value'         => $value,
                    ];
                }

                $partitions[] = [
                    'PartitionId'           => $partitionId,
                    'ErrorCode'             => $errorCode,
                    'HighwaterMarkOffset'   => $highwaterMarkOffset,
                    'MessageSet'            => $messageSet,
                ];
            }

            $topics[] = [
                'Topic'         => $topic,
                'Partitions'    => $partitions,
            ];
        }

        if (!$yield) {
            yield [
                'CorrelationId' => $correlationId,
                'Topics'        => $topics,
            ];
        }
    }
}
