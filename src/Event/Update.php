<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

class Update extends Insert
{
    use CriteriaTrait;

    public function __construct(
        string $table,
        array $data = null,
        array|string $criteria = null,
        array|bool $options = null,
        string $rootEvent = null,
    ) {
        parent::__construct($table, $data, $rootEvent);

        $this->setOptions($options);
        $this->setCriteria($criteria);
    }
}
