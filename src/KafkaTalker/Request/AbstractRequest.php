<?php
namespace KafkaTalker\Request;

use KafkaTalker\Packer;

abstract class AbstractRequest
{
    protected $client;
    protected $correlationId = null;
    protected $debug = false;

    public function __construct($client, $options = [])
    {
        if ($this->debug) {
            printf("%s::__construct\n", get_called_class());
        }

        $this->debug = !empty($options['debug']);

        $this->client = $client;
    }

    protected function buildHeader($options = [])
    {
        $apiVersion = 0;
        if (isset($options['api_version'])) {
            $apiVersion = $options['api_version'];
        }
        $correlationId = isset($this->correlationId)
            ? $this->correlationId
            : mt_rand();
        $clientId = 'hello';

        $packedApiKey = Packer::packSignedInt16(static::API_KEY);
        $packedApiVersion = Packer::packSignedInt16($apiVersion);
        $packedCorrelationId = Packer::packSignedInt32($correlationId);
        $packedClientId = Packer::packSignedInt16(strlen($clientId)) . $clientId;

        $header = $packedApiKey . $packedApiVersion . $packedCorrelationId . $packedClientId;

        return $header;
    }

    public function setCorrelationId($correlationId)
    {
        $this->correlationId = $correlationId;

        return $this;
    }

    public function setDebug($debug)
    {
        $this->debug = (bool) $debug;

        return $this;
    }
}
