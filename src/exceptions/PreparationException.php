<?php

namespace SuperMysqli\Exceptions;

class PreparationException extends \Exception
{
    public function __construct(string $message = "", int $code = 0)
    {
        $msg = 'SQL Statement cannot be prepared';
        if ($message != '')
            $msg .= ' : ' . $message;
        parent::__construct($msg, $code);
    }
}