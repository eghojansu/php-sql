<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

class InsertBatch extends Event
{
    use DataTrait;
    use OptionsTrait;
    use CriteriaTrait;

    public function __construct(
        string $table,
        array $data,
        array|string $criteria = null,
        array|string $options = null,
        string $rootEvent = null,
    ) {
        parent::__construct($table, null, $rootEvent);

        $this->setData($data);
        $this->setOptions($options);
        $this->setCriteria($criteria);
    }
}
