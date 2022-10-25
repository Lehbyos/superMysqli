<?php

namespace SuperMysqli\Exceptions;

class AliasNotFoundException extends \Exception
{
    public function __construct(string $alias = "", $code = 0)
    {
        $msg = 'Alias not found';
        if ($alias != '')
            $msg .= ' (' . $alias . ')';
        parent::__construct($msg, $code);
    }
}