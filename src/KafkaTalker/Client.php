<?php
namespace KafkaTalker;

use KafkaTalker\Exception\KafkaTalkerException;
use KafkaTalker\Logger;

class Client
{
    const MAX_RECONNECT = 5;
    const MAX_WRITE_SIZE = 8192;

    private $apiVersion = 0;
    private $debug = false;
    private $kafkaVersion = null;
    private $socket;
    private $transport = 'stream';

    public function __construct($host, $port, $options = [])
    {
        if (array_key_exists('transport', $options)) {
            if (!in_array($options['transport'], ['socket', 'stream'], true)) {
                // Throw exception
            }
            $this->transport = $options['transport'];
        }
        if (empty($host)) {
            throw new KafkaTalkerException('Missing host');
        }
        if (empty($port)) {
            throw new KafkaTalkerException('Missing port');
        }

        $this->kafkaVersion = !empty($options['kafka_version']) ? $options['kafka_version'] : null;
        $this->apiVersion = 0;
        if ($this->kafkaVersion) {
            if (version_compare($this->kafkaVersion, '0.8.3', '>=')) {
                $this->apiVersion = 2;
            } elseif (version_compare($this->kafkaVersion, '0.8.2', '>=')) {
                $this->apiVersion = 1;
            }
        }

        if ($this->transport === 'socket') {
            $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
            socket_connect($this->socket, $host, $port);
        } elseif ($this->transport === 'stream') {
            $this->socket = fsockopen($host, $port, $errno, $errstr, 6000);

            if ($this->socket === false) {
                // Error
            }

            stream_set_blocking($this->socket, 0);
        }
    }

    public function close()
    {
        Logger::log('[Client::close()] Closing socket handler...');

        $close = fclose($this->socket);

        Logger::log('[Client::close()]     > fclose returned: %s', var_export($close, true));

        return $close;
    }

    public function getApiVersion()
    {
        return $this->apiVersion;
    }

    public function getDebug()
    {
        return $this->debug;
    }

    public function getKafkaVersion()
    {
        return $this->kafkaVersion;
    }

    public function read($length)
    {
        Logger::log('[Client::read()] Reading %d bytes from socket...', var_export($length, true));

        if ($this->transport === 'socket') {
            socket_recv($this->socket, $data, $length, MSG_WAITALL);
        } elseif ($this->transport === 'stream') {
            $read = [$this->socket];
            $readable = stream_select($read, $null, $null, 3000, 3000);

            $retry = 0;

            $data = '';
            while ($length) {
                $buffer = fread($this->socket, $length);
                if ($buffer === false) {
                    // Error
                    Logger::log('[Client::read()] Error: fread returned false');
                } elseif ($buffer) {
                    Logger::log('[Client::read()] fread returned %s', var_export($buffer, true));
                    $data .= $buffer;
                    $length -= strlen($buffer);
                }
            }
        }

        Logger::log('[Client::read()]     > Data read: %s', var_export($data, true));

        return $data;
    }

    public function setApiVersion($apiVersion)
    {
        $this->apiVersion = (int) $apiVersion;

        return $this;
    }

    public function setKafkaVersion($kafkaVersion)
    {
        $this->kafkaVersion = $kafkaVersion;

        return $this;
    }

    public function setDebug($debug)
    {
        $this->debug = (bool) $debug;

        return $this;
    }

    public function write($data)
    {
        Logger::log('[Client::write()] Sending data into socket...');

        $dataSize = strlen($data);
        $written = 0;

        if ($this->transport === 'socket') {
            $written = socket_send($this->socket, $data, strlen($data), 0);
        } elseif ($this->transport === 'stream') {
            $write = [$this->socket];

            while ($written < $dataSize) {
                $writable = stream_select($null, $write, $null, 3000, 3000);
                if ($writable === false) {
                    // Stream is not writable
                } elseif ($writable >= 0) {
                    $w = fwrite($this->socket, substr($data, $written), self::MAX_WRITE_SIZE);
                    if ($w === false) {
                        // Write error
                    }
                    $written += $w;
                } else {
                    // No stream changed
                }
            }
        }

        Logger::log('[Client::write()]     > %d on %d sent bytes', $written, strlen($data));

        return $written;
    }

}
