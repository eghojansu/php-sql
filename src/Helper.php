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

    public function joinCriteria(string|null $criteria, string|null $with, string $conj = null): string
    {
        if (!$with) {
            return $criteria ?? '';
        }

        if ($criteria && $with && !preg_match('/^(and|or)/i', $with)) {
            return ltrim($criteria . ' ' . strtoupper($conj ?? 'AND') . ' (' . $with . ')');
        }

        return trim($criteria . ' ' . $with);
    }

    public function mergeCriteria(array|string|null ...$criteria): array
    {
        return array_reduce(array_filter($criteria), function (array $criteria, $merge) {
            if (is_string($merge)) {
                $criteria[0] = $this->joinCriteria($criteria[0] ?? null, $merge);
            } elseif ($merge) {
                $criteria[0] = $this->joinCriteria($criteria[0] ?? null, $merge[0]);

                array_push($criteria, ...array_slice($merge, 1));
            }

            if (empty($criteria[0])) {
                return array();
            }

            return $criteria;
        }, array());
    }
}
