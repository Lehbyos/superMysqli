<?php

namespace SuperMysqli\Exceptions;

class NoResultException extends \Exception
{
    public function __construct(string $message = "", int $code = 0)
    {
        $msg = 'Cannot get statement result';
        if ($message != '')
            $msg .= ': ' . $message;
        parent::__construct($msg, $code);
    }
}