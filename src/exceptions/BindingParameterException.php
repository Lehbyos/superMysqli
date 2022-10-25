<?php

namespace SuperMysqli\Exceptions;

class BindingParameterException extends \Exception
{
    public function __construct(string $message = '', int $code = 0)
    {
        $msg = 'Parámeters could not be assigned';
        if ($message != '')
            $msg .= ' : ' . $message;

        parent::__construct($msg, $code);
    }
}