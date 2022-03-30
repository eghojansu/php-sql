<?php

declare(strict_types=1);

namespace Ekok\Sql;

use Ekok\Utils\Arr;
use Ekok\Utils\Str;

class Builder
{
    private $options = array(
        'driver' => null,
        'table_prefix' => null,
        'quotes' => null,
        'delimiter' => "\n",
        'raw_identifier' => '"',
    );

    public function __construct(array $options = null)
    {
        $this->setOptions($options ?? array());
    }

    public function getDriver(): string
    {
        return strtolower($this->options['driver'] ?? '');
    }

    public function getTablePrefix(): string|null
    {
        return $this->options['table_prefix'];
    }

    public function getQuotes(): array|string|null
    {
        return $this->options['quotes'];
    }

    public function getDelimiter(): string
    {
        return ($this->options['format_query'] ?? false) ? "\n" : ' ';
    }

    public function getRawIdentifier(): string
    {
        return $this->options['raw_identifier'] ?? '"';
    }

    public function getOptions(): array
    {
        return $this->options;
    }

    public function setOptions(array $options): static
    {
        $this->options = $options + $this->options;

        return $this;
    }

    public function isRaw(string $expr, string &$cut = null): bool
    {
        $raw = str_starts_with($expr, $prefix = $this->getRawIdentifier());

        if ($raw) {
            $cut = substr($expr, strlen($prefix));
        }

        return $raw;
    }

    public function quote(string $expr): string
    {
        return Str::quote($expr, $this->getQuotes(), '.');
    }

    public function raw(string $expr): string
    {
        return $this->getRawIdentifier() . $expr;
    }

    public function table(string $table): string
    {
        return $this->isRaw($table, $name) ? $name : $this->getTablePrefix() . $table;
    }

    public function column(string $column, string $prefix = null): string
    {
        return $this->isRaw($column, $cut) ? $cut : $this->quote((false === strpos($column, '.') && $prefix ? $prefix . '.' : null) . $column);
    }

    public function select(string $table, array|string $criteria = null, array $options = null): array
    {
        $sub = $options['sub'] ?? false;
        $alias = $options['alias'] ?? null;

        if ($sub && !$alias) {
            throw new \LogicException('Sub query needs an alias');
        }

        $sep = $this->getDelimiter();
        $tab = $sub ? $table : $this->table($table);
        $pre = $alias ?? ($sub ? null : $tab);
        $col = $options['columns'] ?? null;
        $val = array();
        $sql = $this->_columns($col ?: '*', $pre, $sep);

        $sql .= $sep . 'FROM ' . ($sub ? '(' . $tab . ')' : $this->quote($tab));

        if ($alias) {
            $sql .= $sep . 'AS ' . $this->quote($alias);
        }

        if ($line = $this->_join($options['joins'] ?? null, $sep)) {
            $sql .= $sep . $line;
        }

        if ($line = $this->_criteria($criteria, $val)) {
            $sql .= $sep . $line;
        }

        if ($line = $this->_order($options['groups'] ?? null, $pre, $sep, 'GROUP BY ')) {
            $sql .= $sep . $line;
        }

        if ($line = $this->_criteria($options['having'] ?? null, $val, 'HAVING ')) {
            $sql .= $sep . $line;
        }

        if ($line = $this->_order($options['orders'] ?? null, $pre, $sep)) {
            $sql .= $sep . $line;
        }

        if ($line = $this->_offset($options['limit'] ?? 0, $options['offset'] ?? 0, !!$line, $top)) {
            $sql = $top ? $line . $sep . $sql : $sql . $sep . $line;
        }

        return array('SELECT' . $sep . $sql, $val);
    }

    public function insert(string $table, array $data, array $options = null): array
    {
        $sep = $this->getDelimiter();

        return array(
            'INSERT INTO ' . $this->quote($this->table($table)) . $sep .
            '(' . $this->_columns(array_keys($data), null, $sep) . ')' . $sep .
            'VALUES' . $sep .
            '(' . str_repeat('?, ', count($data) - 1) . '?)',
            array_values($data),
        );
    }

    public function update(string $table, array $data, array|string $criteria, array $options = null): array
    {
        $sep = $this->getDelimiter();
        $values = array_values($data);
        $withFilter = rtrim($sep . $this->_criteria($criteria, $values));

        return array(
            'UPDATE ' . $this->quote($this->table($table)) . $sep .
            'SET ' . implode(' = ?,' . $sep, array_map(array($this, 'quote'), array_keys($data))) . ' = ?' .
            $withFilter,
            $values,
        );
    }

    public function delete(string $table, array|string $criteria, array $options = null): array
    {
        $values = array();
        $withFilter = rtrim($this->getDelimiter() . $this->_criteria($criteria, $values));

        return array(
            'DELETE FROM ' . $this->quote($this->table($table)) . $withFilter,
            $values,
        );
    }

    public function insertBatch(string $table, array $data, array $options = null): array
    {
        $first = $data[0] ?? null;

        if (!$first || !is_array($first)) {
            throw new \LogicException('No data to be inserted');
        }

        $firstCount = count($first);
        $columns = array_keys($first);
        $line = '(' . str_repeat('?, ', $firstCount - 1) . '?)';
        $sep = $this->getDelimiter();

        $sql = 'INSERT INTO ' . $this->quote($this->table($table)) . $sep . '(' . $this->_columns($columns, null, ' ') . ')' . $sep . 'VALUES ' . $line;
        $values = array();

        foreach ($data as $pos => $row) {
            if (count(array_intersect_key($first, $row)) !== $firstCount) {
                throw new \LogicException(sprintf('Invalid data at position: %s', $pos));
            }

            if ($pos > 0) {
                $sql .= "," . $sep . $line;
            }

            array_push($values, ...Arr::each($columns, static fn($column) => $row[$column]));
        }

        return array($sql, $values);
    }

    public function criteriaJoin(string|null $criteria, string|null $with, string $conj = null): string
    {
        if (!$with) {
            return $criteria ?? '';
        }

        if ($criteria && $with && !preg_match('/^(and|or)/i', $with)) {
            return ltrim($criteria . ' ' . strtoupper($conj ?? 'AND') . ' (' . $with . ')');
        }

        return trim($criteria . ' ' . $with);
    }

    public function criteriaMerge(array|string|null ...$criteria): array
    {
        return array_reduce(array_filter($criteria), function (array $criteria, $merge) {
            if (is_string($merge)) {
                $criteria[0] = $this->criteriaJoin($criteria[0] ?? null, $merge);
            } elseif ($merge) {
                $criteria[0] = $this->criteriaJoin($criteria[0] ?? null, $merge[0]);

                array_push($criteria, ...array_slice($merge, 1));
            }

            if (empty($criteria[0])) {
                return array();
            }

            return $criteria;
        }, array());
    }

    public function criteriaIn(string $column, array $data): array
    {
        if (!$data) {
            throw new \LogicException('Data was empty');
        }

        $params = array_values($data);

        array_unshift($params, ($this->isRaw($column, $cut) ? $cut : $this->quote($column)) . ' IN (' . str_repeat('?, ', count($params) - 1) . '?)');

        return $params;
    }

    private function _columns(string|array $columns, string $prefix = null, string $separator = null): string
    {
        return implode(
            ',' . $separator,
            Arr::each(
                (array) $columns,
                fn (string|null $column, string|int $key) => match(true) {
                    '*' === $column => $column,
                    empty($column) => null,
                    is_numeric($key) => $this->column($column, $prefix),
                    default => $this->column($column, $prefix) . ' AS ' . $this->quote($key),
                },
                true,
            ),
        );
    }

    private function _join(string|array|null $joins, string $separator = null): string
    {
        return implode(
            $separator ?? "\n",
            Arr::each(
                (array) $joins,
                static fn (string|null $line) => match(true) {
                    empty($line) => null,
                    !!preg_match('/^(.+)join/i', $line) => $line,
                    default => 'JOIN ' . $line,
                },
                true,
            ),
        );
    }

    public function _criteria(string|array|null $criteria, array &$values = null, string $prefix = null): string
    {
        $line = $criteria ?? '';

        if (is_array($line)) {
            $tmp = array_shift($line);

            array_walk($line, function ($value) use (&$values) {
                $values[] = $value;
            });

            $line = $tmp ?? '';
        }

        if ($line) {
            $line = ($prefix ?? 'WHERE ') . $line;
        }

        return $line;
    }

    private function _order(string|array|null $orders, string $prefix = null, string $separator = null, string $add = null): string
    {
        if (empty($orders)) {
            return '';
        }

        return ($add ?? 'ORDER BY ') . implode(
            ',' . ($separator ?? ' '),
            Arr::each(
                (array) $orders,
                fn (string|null $column, string|int $key) => match(true) {
                    empty($column) => null,
                    is_numeric($key) => $this->column($column, $prefix),
                    default => $this->column($key, $prefix) . ' ' . strtoupper($column),
                },
                true,
            ),
        );
    }

    private function _offset(int|null $limit, int|null $offset, bool $order, bool &$top = null): string
    {
        $noLimit = 0 >= $limit;
        $noOffset = 0 >= $limit;

        if ($noLimit && $noOffset) {
            return '';
        }

        return match($this->getDriver()) {
            'sqlsrv' => $this->offsetSqlServer($limit, $offset, $order, $top),
            default => trim(($noLimit ? '' : 'LIMIT ' . $limit) . ' ' . ($noOffset ? '' : 'OFFSET ' . $offset)),
        };
    }

    private function offsetSqlServer(int $limit, int $offset, bool $order, bool &$top = null): string
    {
        if (!$order) {
            throw new \LogicException('Offsetting require column order');
        }

        if ($top = !$offset) {
            return 'TOP ' . $limit;
        }

        return 'OFFSET ' . $offset . ' ROWS FETCH NEXT ' . $limit . ' ROWS ONLY';
    }
}
