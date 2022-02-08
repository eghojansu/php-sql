<?php

namespace Ekok\Sql;

use Ekok\Utils\Arr;
use Ekok\Utils\Payload;

/**
 * PDO Sql Connection Wrapper
 */
class Connection
{
    protected $hive = array();
    protected $options = array();

    /** @var string */
    private $driver;

    /** @var string|null */
    private $name;

    /** @var Builder */
    private $builder;

    /** @var array */
    private $maps = array();

    public function __construct(
        protected string $dsn,
        protected string|null $username = null,
        protected string|null $password = null,
        array|null $options = null,
    ) {
        $opt = Arr::merge(array(
            'pagination_size' => 20,
            'format_query' => null,
            'raw_identifier' => null,
            'table_prefix' => null,
            'quotes' => array(),
            'scripts' => array(),
            'options' => array(),
        ), $options);

        $this->driver = strstr($this->dsn, ':', true);
        $this->name = preg_match('/^.+?(?:dbname|database)=(.+?)(?=;|$)/is', $this->dsn, $match) ? str_replace('\\ ', ' ', $match[1]) : null;
        $this->builder = new Builder($this->driver, $opt['table_prefix'], $opt['quotes'], $opt['raw_identifier'], $opt['format_query']);
        $this->options = $opt;
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
        list($sql, $values) = $this->builder->select($table, $criteria, Arr::without($options, 'orders'));
        list($sqlCount) = $this->builder->select($sql, null, array('sub' => true, 'alias' => '_c', 'columns' => array('_d' => $this->builder->raw('COUNT(*)'))));

        return intval($this->query($sqlCount, $values, $success)->fetchColumn(0));
    }

    public function select(string $table, array|string $criteria = null, array $options = null): array|null
    {
        list($sql, $values) = $this->builder->select($table, $criteria, $options);

        $args = $options['fetch_args'] ?? array();
        $fetch = $options['fetch'] ?? \PDO::FETCH_ASSOC;
        $query = $this->query($sql, $values, $success);

        return $success ? (false === ($result = $query->fetchAll($fetch, ...$args)) ? null : $result) : null;
    }

    public function selectOne(string $table, array|string $criteria = null, array $options = null): array|object|null
    {
        return $this->select($table, $criteria, Arr::merge($options, array('limit' => 1)))[0] ?? null;
    }

    public function insert(string $table, array $data, array|string $options = null): bool|int|array|object|null
    {
        list($sql, $values) = $this->builder->insert($table, $data);

        $query = $this->query($sql, $values, $success);

        return $success ? (function () use ($query, $options, $table) {
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
        list($sql, $values) = $this->getBuilder()->update($table, $data, $criteria);

        $query = $this->query($sql, $values, $success);

        return $success ? (false === $options ? $query->rowCount() : $this->selectOne($table, $criteria, true === $options ? null : $options)) : false;
    }

    public function delete(string $table, array|string $criteria): bool|int
    {
        list($sql, $values) = $this->getBuilder()->delete($table, $criteria);

        $query = $this->query($sql, $values, $success);

        return $success ? $query->rowCount() : false;
    }

    public function insertBatch(string $table, array $data, array|string $criteria = null, array|string $options = null): bool|int|array|null
    {
        list($sql, $values) = $this->getBuilder()->insertBatch($table, $data);

        $query = $this->query($sql, $values, $success);

        return $success ? ($criteria ? $this->select($table, $criteria, $options) : $query->rowCount()) : false;
    }

    public function query(string $sql, array $values = null, bool &$success = null): \PDOStatement
    {
        $query = $this->getPdo()->prepare($sql);

        if (!$query) {
            throw new \RuntimeException('Unable to prepare query');
        }

        $result = $query->execute($values);
        $success = $result && '00000' === $query->errorCode();

        return $query;
    }

    public function exec(string $sql, array $values = null): int
    {
        $query = $this->query($sql, $values, $success);

        return $success ? $query->rowCount() : 0;
    }

    public function transact(\Closure $fn)
    {
        $pdo = $this->getPdo();
        $auto = !$pdo->inTransaction();

        if ($auto) {
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

        $out = $pdo->query('SELECT 1 FROM ' . $this->builder->table($table) . ' LIMIT 1');
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, $mode);

        return !!$out;
    }

    public function getBuilder(): Builder
    {
        return $this->builder;
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

    public function getOptions(): array
    {
        return $this->options;
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
        return $this->hive['pdo'] ?? ($this->hive['pdo'] = self::createPDOConnection(
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

    public static function createPDOConnection(
        string $dsn,
        string $username = null,
        string $password = null,
        array $options = null,
        array $scripts = null,
    ): \PDO
    {
        try {
            $pdo = new \PDO($dsn, $username, $password, $options);

            Arr::walk($scripts ?? array(), fn(Payload $script) => $pdo->exec($script->value));

            return $pdo;
        } catch (\Throwable $error) {
            throw new \RuntimeException('Unable to connect database', 0, $error);
        }
    }
}
