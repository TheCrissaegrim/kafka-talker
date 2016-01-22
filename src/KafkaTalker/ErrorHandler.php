<?php
namespace KafkaTalker;

class ErrorHandler
{
    private $errors = [
        0 => [
            'name' => 'NoError',
            'retriable' => false,
            'description' => 'No error--it worked!',
        ],
        -1 => [
            'name' => 'Unknown',
            'retriable' => false,
            'description' => 'An unexpected server error',
        ],
        1 => [
            'name' => 'OffsetOutOfRange',
            'retriable' => false,
            'description' => 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.',
        ],
        2 => [
            'name' => 'InvalidMessage / CorruptMessage',
            'retriable' => true,
            'description' => 'This indicates that a message contents does not match its CRC',
        ],
        3 => [
            'name' => 'UnknownTopicOrPartition',
            'retriable' => true,
            'description' => 'This request is for a topic or partition that does not exist on this broker.',
        ],
        4 => [
            'name' => 'InvalidMessageSize',
            'retriable' => false,
            'description' => 'The message has a negative size',
        ],
        5 => [
            'name' => 'LeaderNotAvailable',
            'retriable' => true,
            'description' => 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.',
        ],
        6 => [
            'name' => 'NotLeaderForPartition',
            'retriable' => true,
            'description' => 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.',
        ],
        7 => [
            'name' => 'RequestTimedOut',
            'retriable' => true,
            'description' => 'This error is thrown if the request exceeds the user-specified time limit in the request.',
        ],
        8 => [
            'name' => 'BrokerNotAvailable',
            'retriable' => false,
            'description' => 'This is not a client facing error and is used mostly by tools when a broker is not alive.',
        ],
        9 => [
            'name' => 'ReplicaNotAvailable',
            'retriable' => false,
            'description' => 'If replica is expected on a broker, but is not (this can be safely ignored).',
        ],
        10 => [
            'name' => 'MessageSizeTooLarge',
            'retriable' => false,
            'description' => 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.',
        ],
        11 => [
            'name' => 'StaleControllerEpochCode',
            'retriable' => false,
            'description' => 'Internal error code for broker-to-broker communication.',
        ],
        12 => [
            'name' => 'OffsetMetadataTooLargeCode',
            'retriable' => false,
            'description' => 'If you specify a string larger than configured maximum for offset metadata',
        ],
        14 => [
            'name' => 'GroupLoadInProgressCode',
            'retriable' => true,
            'description' => 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator.',
        ],
        15 => [
            'name' => 'GroupCoordinatorNotAvailableCode',
            'retriable' => true,
            'description' => 'The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active.',
        ],
        16 => [
            'name' => 'NotCoordinatorForGroupCode',
            'retriable' => true,
            'description' => 'The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for.',
        ],
        17 => [
            'name' => 'InvalidTopicCode',
            'retriable' => false,
            'description' => 'For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic).',
        ],
        18 => [
            'name' => 'RecordListTooLargeCode',
            'retriable' => false,
            'description' => 'If a message batch in a produce request exceeds the maximum configured segment size.',
        ],
        19 => [
            'name' => 'NotEnoughReplicasCode',
            'retriable' => true,
            'description' => 'Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1.',
        ],
        20 => [
            'name' => 'NotEnoughReplicasAfterAppendCode',
            'retriable' => true,
            'description' => 'Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required.',
        ],
        21 => [
            'name' => 'InvalidRequiredAcksCode',
            'retriable' => false,
            'description' => 'Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0).'
        ],
        22 => [
            'name' => 'IllegalGenerationCode',
            'retriable' => false,
            'description' => 'Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation.',
        ],
        23 => [
            'name' => 'InconsistentGroupProtocolCode',
            'retriable' => false,
            'description' => 'Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.',
        ],
        24 => [
            'name' => 'InvalidGroupIdCode',
            'retriable' => false,
            'description' => 'Returned in join group when the groupId is empty or null.',
        ],
        25 => [
            'name' => 'UnknownMemberIdCode',
            'retriable' => false,
            'description' => 'Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation.',
        ],
        26 => [
            'name' => 'InvalidSessionTimeoutCode',
            'retriable' => false,
            'description' => 'Return in join group when the requested session timeout is outside of the allowed range on the broker',
        ],
        27 => [
            'name' => 'RebalanceInProgressCode',
            'retriable' => false,
            'description' => 'Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group.',
        ],
        28 => [
            'name' => 'InvalidCommitOffsetSizeCode',
            'retriable' => false,
            'description' => 'This error indicates that an offset commit was rejected because of oversize metadata.',
        ],
        29 => [
            'name' => 'TopicAuthorizationFailedCode',
            'retriable' => false,
            'description' => 'Returned by the broker when the client is not authorized to access the requested topic.',
        ],
        30 => [
            'name' => 'GroupAuthorizationFailedCode',
            'retriable' => false,
            'description' => 'Returned by the broker when the client is not authorized to access a particular groupId.',
        ],
        31 => [
            'name' => 'ClusterAuthorizationFailedCode',
            'retriable' => false,
            'description' => 'Returned by the broker when the client is not authorized to use an inter-broker or administrative API.',
        ],
    ];

    public static function getErrorFromCode($errorCode)
    {
        if (isset($this->errors[$errorCode])) {
            return [
                'code' => $errorCode,
                'message' => $this->errors[$errorCode],
            ];
        }

        return false;
    }
}
