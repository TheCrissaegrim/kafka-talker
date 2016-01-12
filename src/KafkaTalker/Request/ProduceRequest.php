<?php
namespace KafkaTalker\Request;

use KafkaTalker\Packer;

class ProduceRequest extends AbstractRequest
{
    const API_KEY = 0;

    public function send($requiredAcks, $timeout, $topics)
    {
        // Add header (ApiKey, ApiVersion, CorrelationId, ClientId)
        $data = $this->buildHeader();

        // Add RequiredAcks
        $data .= Packer::packSignedInt16($requiredAcks);

        // Add Timeout
        $data .= Packer::packSignedInt32($timeout);

        // Add Topic count
        $data .= Packer::packSignedInt32(count($topics));

        // Add Topics
        foreach ($topics as $topic => $partitions) {
            // Add Topic
            $data .= Packer::packStringSignedInt16($topic);

            // Add Partition count
            $data .= Packer::packSignedInt32(count($partitions));

            // Add Partitions
            foreach ($partitions as $partition => $messages) {
                $data .= Packer::packSignedInt32($partition);

                $packedMessageSet = '';
                foreach ($messages as $message) {
                    if (!is_array($message)) {
                        $offset = 0;        // Producer does not know message offset, so we can fill with any value (we choose 0)
                        $magicByte = 0;     // Used to ensure backwards compatibility. Currently set to 0;
                        $attributes = 0;
                        $key = null;
                    } else {
                        $offset = array_key_exists('Offset', $message) ? $message['Offset'] : 0;
                        $magicByte = array_key_exists('MagicByte', $message) ? $message['MagicByte'] : 0;
                        $attributes = array_key_exists('Attributes', $message) ? $message['Attributes'] : 0;
                        $attributes = 0;    // TODO: handle compression
                        $key = array_key_exists('Key', $message) ? $message['Key'] : null;
                    }

                    $packedMessage = Packer::packSignedInt8($magicByte);
                    $packedMessage .= Packer::packSignedInt8($attributes);
                    $packedMessage .= Packer::packStringSignedInt32($key);
                    $packedMessage .= Packer::packStringSignedInt32($message);

                    $packedMessage = Packer::packSignedInt32(crc32($packedMessage)) . $packedMessage;

                    $packedMessageSet .= Packer::packSignedInt64($offset) . Packer::packStringSignedInt32($packedMessage);
                }

                // Add MessageSet
                $data .= Packer::packStringSignedInt32($packedMessageSet);
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
            if ($this->debug) {
                printf("    > [Topic #%d]\n", $i);
            }

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

            // Read Partition count
            $partitionCount = Packer::unpackSignedInt32(substr($response, $cursor, 4));
            if ($this->debug) {
                printf("        > Partition count: %s\n", var_export($partitionCount, true));
            }
            $cursor += 4;

            // Read Partitions
            $partitions = [];
            for ($j = 1; $j <= $partitionCount; $j++) {
                if ($this->debug) {
                    printf("            > [Partition #%d]\n", $j);
                }

                // Read Partition
                $partitionId = Packer::unpackSignedInt32(substr($response, $cursor, 4));
                if ($this->debug) {
                    printf("                > PartitionId: %s\n", var_export($partitionId, true));
                }
                $cursor += 4;

                $errorCode = Packer::unpackSignedInt16(substr($response, $cursor, 2));
                if ($this->debug) {
                    printf("                > ErrorCode: %s\n", var_export($errorCode, true));
                }
                $cursor += 2;

                $offset = Packer::unpackSignedInt64(substr($response, $cursor, 8));
                if ($this->debug) {
                    printf("                > Offset: %s\n", var_export($offset, true));
                }
                $cursor += 8;

                $partitions[] = [
                    'PartitionId'   => $partitionId,
                    'ErrorCode'     => $errorCode,
                    'Offset'        => $offset,
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
