<?php

namespace Ekok\Sql;

use Ekok\Utils\Str;

class Helper
{

    private $quotes = array();
    private $rawIdentifier = '"';

    public function __construct(
        string|array $quotes = null,
        string|null $rawIdentifier = null,
        private string|null $tablePrefix = null,
    ) {
        if ($quotes) {
            $this->quotes = array_slice(is_array($quotes) ? array_values($quotes) : str_split($quotes), 0, 2);
        }

        if ($rawIdentifier) {
            $this->rawIdentifier = $rawIdentifier;
        }
    }

    public function quote(string $expr): string
    {
        return Str::quote($expr, ...$this->quotes);
    }

    public function isRaw(string $expr, string &$cut = null): bool
    {
        $raw = str_starts_with($expr, $this->rawIdentifier);

        if ($raw) {
            $cut = substr($expr, strlen($this->rawIdentifier));
        }

        return $raw;
    }

    public function raw(string $expr): string
    {
        return $this->rawIdentifier . $expr;
    }

    public function table(string $table): string
    {
        return $this->isRaw($table, $name) ? $name : $this->tablePrefix . $table;
    }
}
