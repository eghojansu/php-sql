<?php

declare(strict_types=1);

namespace Ekok\Sql;

use Ekok\Utils\Arr;
use Ekok\Logger\Log;

/**
 * PDO Sql Connection Wrapper
 */
class Connection
{
    /** @var Builder */
    private $builder;

    /** @var Log */
    private $log;

    /** @var string */
    private $dsn;

    /** @var string */
    private $username;

    /** @var string */
    private $password;

    /** @var string */
    private $driver;

    /** @var string|null */
    private $name;

    /** @var array */
    private $maps = array();

    /** @var array */
    private $options = array(
        'pagination_size' => 20,
        'format_query' => null,
        'raw_identifier' => null,
        'table_prefix' => null,
        'quotes' => null,
        'scripts' => null,
        'options' => null,
    );

    public function __construct(
        Log $log,
        string $dsn,
        string $username = null,
        string $password = null,
        array $options = null,
    ) {
        $this->log = $log;
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->driver = strstr($dsn, ':', true);
        $this->name = self::parseDbName($dsn);

        $this->setOptions($options ?? array());
    }

    public function simplePaginate(string $table, int $page = 1, array|string $criteria = null, array $options = null): array
    {
        return $this->paginate($table, $page, $criteria, Arr::merge($options, array('full' => false)));
    }

    public function paginate(string $table, int $page = 1, array|string $criteria = null, array $options = null): array
    {
        $current_page = max($page, 1);
        $last_page = $current_page + 1;
        $per_page = intval($options['limit'] ?? $this->options['pagination_size']);
        $offset = ($current_page - 1) * $per_page;
        $total = null;
        $empty = false;

        if ($options['full'] ?? true) {
            $total = $this->count($table, $criteria, Arr::without($options, 'limit'));
            $empty = $total === 0;
            $last_page = intval(ceil($total / $per_page));
        }

        $subset = null === $total || $total > 0 ? $this->select($table, $criteria, Arr::merge($options, array('limit' => $per_page), compact('offset'))) : array();
        $count = count($subset);
        $next_page = min($current_page + 1, $last_page);
        $prev_page = max($current_page - 1, 0);
        $first = $offset + 1;
        $last = max($first, $offset + $count);

        return compact('subset', 'empty', 'count', 'total', 'current_page', 'next_page', 'prev_page', 'last_page', 'per_page', 'first', 'last');
    }

    public function count(string $table, array|string $criteria = null, array $options = null): int
    {
        $builder = $this->getBuilder();

        list($sql, $values) = $builder->select($table, $criteria, Arr::without($options, 'orders'));
        list($sqlCount) = $builder->select($sql, null, array('sub' => true, 'alias' => '_c', 'columns' => array('_d' => $builder->raw('COUNT(*)'))));

        return $this->query($sqlCount, $values, $query) ? intval($query->fetchColumn(0)) : 0;
    }

    public function select(string $table, array|string $criteria = null, array $options = null): array|null
    {
        list($sql, $values) = $this->getBuilder()->select($table, $criteria, $options);

        $args = $options['fetch_args'] ?? array();
        $fetch = $options['fetch'] ?? \PDO::FETCH_ASSOC;

        return $this->query($sql, $values, $query) ? (false === ($result = $query->fetchAll($fetch, ...$args)) ? null : $result) : null;
    }

    public function selectOne(string $table, array|string $criteria = null, array $options = null): array|object|null
    {
        return $this->select($table, $criteria, Arr::merge($options, array('limit' => 1)))[0] ?? null;
    }

    public function save(string $table, array $data, array|string $criteria = null, array|bool|null $options = false): bool|int|array|object|null
    {
        if ($criteria && $this->selectOne($table, $criteria, (array) $options)) {
            return $this->update($table, $data, $criteria, $options);
        }

        return $this->insert($table, $data, is_bool($options) ? null : $options);
    }

    public function insert(string $table, array $data, array|string $options = null): bool|int|array|object|null
    {
        list($sql, $values) = $this->getBuilder()->insert($table, $data, (array) $options);

        return $this->query($sql, $values, $query) ? (function () use ($query, $options, $table) {
            if (!$options || (is_array($options) && !($load = $options['load'] ?? null))) {
                return $query->rowCount();
            }

            if (isset($load)) {
                $loadOptions = $options;
            } else {
                $loadOptions = null;
                $load = $options;
            }

            $criteria = is_string($load) ? array($load . ' = ?') : (array) $load;
            $criteria[] = $this->getPdo()->lastInsertId();

            return $this->selectOne($table, $criteria, $loadOptions);
        })() : false;
    }

    public function update(string $table, array $data, array|string $criteria, array|bool|null $options = false): bool|int|array|object|null
    {
        list($sql, $values) = $this->getBuilder()->update($table, $data, $criteria, (array) $options);

        return $this->query($sql, $values, $query) ? (false === $options ? $query->rowCount() : $this->selectOne($table, $criteria, true === $options ? null : $options)) : false;
    }

    public function delete(string $table, array|string $criteria, array|bool|null $options = null): bool|int|array|object|null
    {
        $result = true === $options || true === ($options['load'] ?? false) ? $this->select($table, $criteria, (array) $options) : null;

        list($sql, $values) = $this->getBuilder()->delete($table, $criteria, (array) $options);

        return $this->query($sql, $values, $query) ? ($result ?? $query->rowCount()) : false;
    }

    public function insertBatch(string $table, array $data, array|string $criteria = null, array|string $options = null): bool|int|array|null
    {
        list($sql, $values) = $this->getBuilder()->insertBatch($table, $data, (array) $options);

        return $this->query($sql, $values, $query) ? ($criteria ? $this->select($table, $criteria, $options) : $query->rowCount()) : false;
    }

    public function query(string $sql, array $values = null, \PDOStatement &$query = null): bool
    {
        try {
            $query = $this->getPdo()->prepare($sql);
            $success = $query ? $query->execute($values) : false;

            return $success && '00000' === $query->errorCode();
        } catch (\Throwable $error) {
            $this->log->log(Log::LEVEL_ERROR, $error->getMessage(), compact('sql', 'values') + array(
                'query' => $this->stringify($sql, $values),
                'trace' => Arr::formatTrace($error),
            ));

            return false;
        }
    }

    public function exec(string $sql, array $values = null): int
    {
        return $this->query($sql, $values, $query) ? $query->rowCount() : 0;
    }

    public function transact(\Closure $fn)
    {
        $pdo = $this->getPdo();

        if ($auto = !$pdo->inTransaction()) {
            $pdo->beginTransaction();
        }

        $result = $fn($this);

        if ($auto) {
            $endTransaction = '00000' === $pdo->errorCode() ? 'commit' : 'rollBack';

            $pdo->$endTransaction();
        }

        return $result;
    }

    public function lastId(string $name = null): string|false
    {
        return $this->getPdo()->lastInsertId($name);
    }

    public function exists(string $table): bool
    {
        $pdo = $this->getPdo();
        $mode = $pdo->getAttribute(\PDO::ATTR_ERRMODE);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_SILENT);

        $out = $pdo->query('SELECT 1 FROM ' . $this->getBuilder()->table($table) . ' LIMIT 1');
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, $mode);

        return !!$out;
    }

    public function getBuilder(): Builder
    {
        return $this->builder ?? $this->setBuilder(new Builder($this->options))->builder;
    }

    public function setBuilder(Builder $builder): static
    {
        $this->builder = $builder;

        return $this;
    }

    public function map(string $name): Mapper
    {
        $setup = $this->maps[$name] ?? null;

        if (!$setup) {
            return new Mapper($this, $name);
        }

        return new $setup['mapper']($this, ...$setup['arguments']);
    }

    public function addMap(string $name, string $classTable = null, ...$arguments): static
    {
        $mapper = $classTable && class_exists($classTable) ? $classTable : Mapper::class;

        $this->maps[$name] = compact('mapper', 'arguments');

        if (Mapper::class === $mapper) {
            array_unshift($this->maps[$name]['arguments'], $classTable ?? $name);
        }

        return $this;
    }

    public function stringify(string $sql, array $values = null): string
    {
        $text = $sql;

        if ($values) {
            $search = array();
            $replace = array();
            $pdo = $this->getPdo();

            array_walk($values, function ($value, $key) use ($pdo, &$search, &$replace) {
                $search[] = '/' . preg_quote(is_numeric($key) ? chr(0) . '?' : $key) . '/';
                $replace[] = is_string($value) ? $pdo->quote($value) : (is_scalar($value) ? var_export($value, true) : $pdo->quote((string) $value));
            });

            $text = preg_replace($search, $replace, str_replace('?', chr(0) . '?', $sql), 1);
        }

        return $text;
    }

    public function getOption(string $name)
    {
        return $this->options[$name] ?? null;
    }

    public function setOption(string $name, $value): static
    {
        $this->options[$name] = $value;

        return $this;
    }

    public function getOptions(): array
    {
        return $this->options;
    }

    public function setOptions(array $options): static
    {
        $this->options = $options + $this->options + array('driver' => $this->driver);

        return $this;
    }

    public function getVersion(): string
    {
        return $this->getPdo()->getAttribute(\PDO::ATTR_SERVER_VERSION);
    }

    public function getDriver(): string
    {
        return $this->driver;
    }

    public function getName(): string|null
    {
        return $this->name;
    }

    public function getPdo(): \PDO
    {
        return $this->hive['pdo'] ?? ($this->hive['pdo'] = $this->createPDOConnection(
            $this->dsn,
            $this->username,
            $this->password,
            $this->options['options'],
            $this->options['scripts'],
        ));
    }

    public function __clone()
    {
        throw new \LogicException('Cloning Connection is prohibited');
    }

    public function createPDOConnection(
        string $dsn,
        string $username = null,
        string $password = null,
        array $options = null,
        array $scripts = null,
    ): \PDO {
        try {
            $pdo = new \PDO($dsn, $username, $password, $options);

            Arr::each($scripts ?? array(), fn($script) => $pdo->exec($script));

            return $pdo;
        } catch (\Throwable $error) {
            $this->log->log(Log::LEVEL_ERROR, $error->getMessage(), Arr::formatTrace($error));

            throw new \RuntimeException('Unable to connect database', 0, $error);
        }
    }

    private static function parseDbName(string $dsn): string|null
    {
        return preg_match('/^.+?(?:dbname|database)=(.+?)(?=;|$)/is', $dsn, $match) ? str_replace('\\ ', ' ', $match[1]) : null;
    }
}
