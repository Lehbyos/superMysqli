<?php

namespace SuperMysqli\Exceptions;

class ConnectionException extends \Exception
{
    public function __construct(string $server, string $database, string $user, string $password, string $message = "")
    {
        parent::__construct('Connection Error: ' . $message . "\n" . $server . ' -- ' . $database . ' - ' . $user . ' - ' . $password);
    }
}