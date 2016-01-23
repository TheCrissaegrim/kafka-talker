<?php
namespace KafkaTalker\Request;

use KafkaTalker\Packer;

abstract class AbstractRequest
{
    const DEFAULT_API_VERSION = 0;
    const DEFAULT_CLIENT = 'kafka-talker';

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

    /**
     * Build request header
     * @param  array  $options [description]
     * @return [type]          [description]
     */
    protected function buildHeader($options = [])
    {
        // Set API version if passed, else default to DEFAULT_API_VERSION
        $apiVersion = isset($options['api_version'])
            ? $options['api_version']
            : self::DEFAULT_API_VERSION;
        // Set correlation ID if passed, else generate a random one
        $correlationId = isset($this->correlationId)
            ? $this->correlationId
            : mt_rand();
        // Set client if passed, else default to DEFAULT_CLIENT
        $clientId = isset($options['client_id'])
            ? $options['client_id']
            : self::DEFAULT_CLIENT;

        $header = Packer::packSignedInt16(static::API_KEY)
            . Packer::packSignedInt16($apiVersion)
            . Packer::packSignedInt32($correlationId)
            . Packer::packStringSignedInt16($clientId);

        return $header;
    }

    /**
     * Get request correlation ID
     * @return integer  Request correlation ID
     */
    public function getCorrelationId()
    {
        return $this->correlationId;
    }

    /**
     * Get debug mode state
     * @return boolean  Debug mode state
     */
    public function getDebug($debug)
    {
        return $this->debug;
    }

    /**
     * Set request correlation ID
     * @param integer   $correlationId  Request correlation ID
     */
    public function setCorrelationId($correlationId = null)
    {
        $this->correlationId = $correlationId;

        return $this;
    }

    /**
     * Set debug mode on/off
     * @param boolean   $debug  True to enable debug, false to disable
     */
    public function setDebug($debug)
    {
        $this->debug = (bool) $debug;

        return $this;
    }
}
