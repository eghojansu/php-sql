<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

class Delete extends Event
{
    use CriteriaTrait;
    use OptionsTrait;

    public function __construct(
        string $table,
        array|string $criteria = null,
        array|bool|null $options = null,
        string $rootEvent = null,
    ) {
        parent::__construct($table, null, $rootEvent);

        $this->setCriteria($criteria);
        $this->setOptions($options);
    }
}
