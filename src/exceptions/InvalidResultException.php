<?php

namespace SuperMysqli\Exceptions;

class InvalidResultException extends \Exception
{
    public function __construct(string $message = "", int $code = 0)
    {
        $msg = 'Invalid result from statement';
        if ($message != '')
            $msg .= ': ' . $message;
        parent::__construct($msg, $code);
    }
}