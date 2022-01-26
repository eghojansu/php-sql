<?php

use Ekok\Sql\Mapper;
use Ekok\Sql\Connection;

final class UserMap extends Mapper
{
    private $value;

    public function __construct(Connection $db, bool $readonly = false)
    {
        $casts = array(
            'prop_arr' => 'arr',
            'prop_json' => 'json',
            'prop_int' => 'int',
            'prop_float' => 'float',
            'prop_bool' => 'bool',
            'prop_str' => 'str',
            'prop_date' => 'date',
            'prop_datetime' => 'datetime',
        );

        parent::__construct(
            $db,
            null,
            array('username' => false),
            $casts,
            $readonly,
            array_merge(array_keys($casts), array('username', 'name')),
            'ignored_column',
        );
    }

    public function getValue()
    {
        return $this->value;
    }

    public function setValue($value)
    {
        $this->value = $value;
    }

    public function removeValue()
    {
        $this->value = null;
    }

    public function isActive()
    {
        return !!$this->value;
    }

    public static function tableSchema(): string
    {
        return <<<'SQL'
CREATE TABLE "user_map" (
    "username" VARCHAR(64) NOT NULL,
    "name" VARCHAR(64) NOT NULL,
    "prop_arr" VARCHAR(255) NULL,
    "prop_json" VARCHAR(255) NULL,
    "prop_int" VARCHAR(255) NULL,
    "prop_float" DECIMAL(19, 15) NULL,
    "prop_bool" VARCHAR(255) NULL,
    "prop_str" VARCHAR(255) NULL,
    "prop_date" DATE NULL,
    "prop_datetime" DATETIME NULL,
    "ignored_column" VARCHAR(255) NULL
)
SQL;
    }

    public static function generateRow(string $username, string $name = null): array
    {
        return array(
            'username' => $username,
            'name' => $name ?? $username,
            'prop_arr' => range($start = random_int(1, 10), $start + random_int(3, 7)),
            'prop_json' => array('foo' => random_int(1, 3), 'bar' => random_int(4, 6)),
            'prop_int' => random_int(1, 1000),
            'prop_float' => round(random_int(1, 1000) / random_int(2, 7), 5),
            'prop_bool' => (bool) random_int(0, 1),
            'prop_str' => bin2hex(random_bytes(5)) . str_repeat(' ', random_int(0, 17)),
            'prop_date' => date('Y-m-d', time() + random_int(1000, 10000)),
            'prop_datetime' => date('Y-m-d H:i:s', time() + random_int(1000, 10000)),
            'ignored_column' => bin2hex(random_bytes(5)),
        );
    }
}
