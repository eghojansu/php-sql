<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

use Ekok\Utils\Str;
use Ekok\EventDispatcher\Event as EventBase;

abstract class Event extends EventBase
{
    private $table;
    private $result;
    private $rootEvent;

    public function __construct(string $table, array|int|object $result = null, string $rootEvent = null)
    {
        $this->rootEvent = $rootEvent;

        $this->setTable($table);
        $this->setResult($result);
    }

    public function getName(): ?string
    {
        return 'on' . Str::className(static::class);
    }

    public function getRootEvent(): string
    {
        return $this->rootEvent ?? static::class;
    }

    public function getTable(): string
    {
        return $this->table;
    }

    public function setTable(string $table): static
    {
        $this->table = $table;

        return $this;
    }

    public function getResult(): bool|int|array|object|null
    {
        return $this->result;
    }

    public function setResult(bool|int|array|object|null $result): static
    {
        $this->result = $result;

        return $this;
    }

    public function getResultAs(string $type, $default = null)
    {
        $isType = 'is_' . $type;

        return $isType($this->result) ? $this->result : $default;
    }
}
