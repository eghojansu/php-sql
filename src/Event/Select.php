<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

class Select extends Event
{
    use CriteriaTrait;
    use OptionsTrait;

    public function __construct(
        string $table,
        string|array $criteria = null,
        array $options = null,
        string $rootEvent = null,
    ) {
        parent::__construct($table, $rootEvent);

        $this->setCriteria($criteria);
        $this->setOptions($options);
    }
}
