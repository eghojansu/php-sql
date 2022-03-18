<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

use Ekok\Utils\Str;
use Ekok\EventDispatcher\Event as EventBase;

abstract class Event extends EventBase
{
    private $table;
    private $result;

    public function __construct(string $table, $result = null, string $rootEvent = null)
    {
        $this->rootEvent = $rootEvent;

        $this->setTable($table);
        $this->setResult($result);
    }

    public function getName(): ?string
    {
        return 'on' . Str::className(static::class);
    }

    public function getRootEvent(): string|null
    {
        return $this->rootEvent;
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

    public function getResult()
    {
        return $this->result;
    }

    public function setResult($result): static
    {
        $this->result = $result;

        return $this;
    }
}
