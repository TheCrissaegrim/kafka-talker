<?php
namespace KafkaTalker;

use KafkaTalker\Exception\KafkaTalkerException;
use KafkaTalker\Logger;

class Client
{
    private $apiVersion = 0;
    private $host = null;
    private $kafkaVersion = null;
    private $maxReadRetry = 5;
    private $maxWriteRetry = 5;
    private $port = null;
    private $readRetryInterval = 500;
    private $reconnectOnFail = false;
    private $socket;
    private $transport = 'stream';
    private $writeRetryInterval = 500;
    private $writeBufferSize = 8192;

    public function __construct($host, $port)
    {
        $this->setHost($host);
        $this->setPort($port);
    }

    public function close()
    {
        Logger::log('[Client::close()] Closing socket handler...');

        $close = fclose($this->socket);

        Logger::log('[Client::close()]     > fclose returned: %s', var_export($close, true));

        return $close;
    }

    public function connect()
    {
        if (empty($this->host)) {
            throw new KafkaTalkerException('Missing host', 0);
        }
        if (empty($this->port)) {
            throw new KafkaTalkerException('Missing port', 0);
        }

        if ($this->transport === 'socket') {
            $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
            socket_connect($this->socket, $this->host, $this->port);
        } else {
            $this->socket = fsockopen($this->host, $this->port, $errno, $errstr, 6000);

            if ($this->socket === false) {
                throw new KafkaTalkerException(sprintf('Connection to %s:%d failed.', $this->host, $this->port), 0);
            }

            stream_set_blocking($this->socket, 0);
        }

        return true;
    }

    public function getApiVersion()
    {
        return $this->apiVersion;
    }

    public function getHost()
    {
        return $this->host;
    }

    public function getKafkaVersion()
    {
        return $this->kafkaVersion;
    }

    public function getMaxReadRetry()
    {
        return $this->maxReadRetry;
    }

    public function getMaxWriteRetry()
    {
        return $this->maxWriteRetry;
    }

    public function getPort()
    {
        return $this->port;
    }

    public function getReadRetryInterval()
    {
        return $this->readRetryInterval;
    }

    public function getReconnectOnFail()
    {
        return $this->reconnectOnFail;
    }

    public function getTransport()
    {
        return $this->transport;
    }

    public function getWriteBufferSize()
    {
        return $this->writeBufferSize;
    }

    public function getWriteRetryInterval()
    {
        return $this->writeRetryInterval;
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
                if (($buffer === false) || ($buffer === '')) {
                    Logger::log('[Client::read()] fread failed');

                    if ($retry === $this->maxReadRetry) {
                        Logger::log('[Client::read()] Max read retry (%d) reached', $retry);
                        $reconnect = false;
                        if ($this->reconnectOnFail) {
                            Logger::log('[Client::read()] AutoReconnectOnFail is on. Trying to reconnect to Kafka server...');
                            $reconnect = $this->reconnect();
                            if ($reconnect) {
                                Logger::log('[Client::read()]     > Reconnection successed');
                                $retry = 0;
                            } else {
                                Logger::log('[Client::read()]     > Reconnection failed');
                            }
                        }
                        if (!$reconnect) {
                            $this->close();
                            throw new KafkaTalkerException(sprintf('Socket read max retry reached (%d). Socket has been closed.', $retry), 0);
                        }
                    } else {
                        Logger::log('[Client::read()] Retry %d on %d in %d milliseconds', $retry, $this->readRetryInterval, $this->readRetryInterval);
                        usleep($this->writeRetryInterval);
                        $retry++;
                    }
                } elseif ($buffer) {
                    $retry = 0;
                    Logger::log('[Client::read()] fread returned %s', var_export($buffer, true));
                    $data .= $buffer;
                    $length -= strlen($buffer);
                }
            }
        }

        Logger::log('[Client::read()]     > Data read: %s', var_export($data, true));

        return $data;
    }

    public function reconnect()
    {
        $this->close();

        return $this->connect();
    }

    public function setHost($host)
    {
        $this->host = (string) $host;

        return $this;
    }

    public function setKafkaVersion($kafkaVersion)
    {
        $this->kafkaVersion = (string) $kafkaVersion;
        $this->apiVersion = 0;
        if ($this->kafkaVersion) {
            if (version_compare($this->kafkaVersion, '0.8.3', '>=')) {
                $this->apiVersion = 2;
            } elseif (version_compare($this->kafkaVersion, '0.8.2', '>=')) {
                $this->apiVersion = 1;
            }
        }

        return $this;
    }

    public function setMaxReadRetry($maxReadRetry)
    {
        $this->maxReadRetry = (integer) $maxReadRetry;

        return $this;
    }

    public function setMaxWriteRetry($maxWriteRetry)
    {
        $this->maxWriteRetry = (integer) $maxWriteRetry;

        return $this;
    }

    public function setPort($port)
    {
        $this->port = (integer) $port;

        return $this;
    }

    public function setReadRetryInterval($readRetryInterval)
    {
        $this->readRetryInterval = (integer) $readRetryInterval;

        return $this;
    }

    public function setReconnectOnFail($reconnectOnFail)
    {
        $this->reconnectOnFail = (boolean) $reconnectOnFail;

        return $this;
    }

    public function setTransport($transport)
    {
        if (!in_array($transport, ['socket', 'stream'], true)) {
            throw new KafkaTalkerException('Invalid transport option (available values: "socket", "stream")', 0);
        }
        $this->transport = $transport;

        return $this;
    }

    public function setWriteBufferSize($writeBufferSize)
    {
        $this->writeBufferSize = (integer) $writeBufferSize;

        return $this;
    }

    public function setWriteRetryInterval($writeRetryInterval)
    {
        $this->writeRetryInterval = (integer) $writeRetryInterval;

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

            $retry = 0;
            while ($written < $dataSize) {
                $writable = stream_select($null, $write, $null, 3000, 3000);

                // Stream is not writable
                if ($writable === false) {
                // Stream is writable
                } elseif ($writable >= 0) {
                    $w = fwrite($this->socket, substr($data, $written), $this->writeBufferSize);

                    // Write failed
                    if (($w === false) || ($w === 0)) {
                        Logger::log('[Client::write()] Write failed');
                        $reconnect = false;
                        if ($retry === $this->maxWriteRetry) {
                            Logger::log('[Client::write()] Max write retry (%d) reached.', $retry);
                            if ($this->reconnectOnFail) {
                                Logger::log('[Client::write()] AutoReconnectOnFail is on. Trying to reconnect to Kafka server...');
                                $reconnect = $this->reconnect();
                                if ($reconnect) {
                                    Logger::log('[Client::write()]     > Reconnection successed');
                                    $retry = 0;
                                } else {
                                    Logger::log('[Client::write()]     > Reconnection failed');
                                }
                            }
                            if (!$reconnect) {
                                $this->close();
                                throw new KafkaTalkerException(sprintf('Socket max write retry reached (%d). Socket has been closed.', $retry), 0);
                            }
                        } else {
                            Logger::log('[Client::write()] Retry %d on %d in %d milliseconds', $retry, $this->writeRetryInterval, $this->writeRetryInterval);
                            usleep($this->writeRetryInterval);
                            $retry++;
                        }
                    } else {
                        $written += $w;
                    }
                } else {
                    // No stream changed
                }
            }
        }

        Logger::log('[Client::write()]     > %d on %d sent bytes', $written, strlen($data));

        return $written;
    }

}
