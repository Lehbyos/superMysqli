<?php

namespace SuperMysqli\Exceptions;

class AliasAlreadyDefinedException extends \Exception
{
    public function __construct(string $message = "", int $code = 0)
    {
        $msg = 'Alias already defined';
        if ($message != '')
            $msg .= '(' . $message . ')';

        parent::__construct($msg, $code);
    }
}