<?php

declare(strict_types=1);

namespace Ekok\Sql;

use Ekok\Logger\Log;
use Ekok\EventDispatcher\Event;
use Ekok\EventDispatcher\Dispatcher;
use Ekok\Sql\Event\Count as CountEvent;
use Ekok\Sql\Event\Delete as DeleteEvent;
use Ekok\Sql\Event\Insert as InsertEvent;
use Ekok\Sql\Event\Select as SelectEvent;
use Ekok\Sql\Event\Update as UpdateEvent;
use Ekok\Sql\Event\Paginate as PaginateEvent;
use Ekok\Sql\Event\AfterCount as AfterCountEvent;
use Ekok\Sql\Event\AfterDelete as AfterDeleteEvent;
use Ekok\Sql\Event\AfterInsert as AfterInsertEvent;
use Ekok\Sql\Event\AfterSelect as AfterSelectEvent;
use Ekok\Sql\Event\AfterUpdate as AfterUpdateEvent;
use Ekok\Sql\Event\InsertBatch as InsertBatchEvent;
use Ekok\Sql\Event\AfterPaginate as AfterPaginateEvent;
use Ekok\Sql\Event\AfterInsertBatch as AfterInsertBatchEvent;

class ListenableConnection extends Connection
{
    /** @var Dispatcher */
    private $dispatcher;

    public function __construct(
        Dispatcher $dispatcher,
        Log $log,
        string $dsn,
        string $username = null,
        string $password = null,
        array $options = null,
    ) {
        parent::__construct($log, $dsn, $username, $password, $options);

        $this->dispatcher = $dispatcher;
    }

    public function listen(string $eventName, callable|string $handler, int $priority = null, bool $once = false): static
    {
        $this->dispatcher->on($eventName, $handler, $priority, $once);

        return $this;
    }

    public function unlisten(string $eventName, int $pos = null): static
    {
        $this->dispatcher->off($eventName, $pos);

        return $this;
    }

    public function dispatch(Event $event, string $eventName = null, bool $once = false): static
    {
        $this->dispatcher->dispatch($event, $eventName, $once);

        return $this;
    }

    public function paginate(string $table, int $page = 1, array|string $criteria = null, array $options = null): array
    {
        $event = new PaginateEvent($table, $page, $criteria, array(
            'root_event' => PaginateEvent::class,
        ) + ($options ?? array()));

        $this->dispatch($event);

        if ($event->isPropagationStopped()) {
            return $event->getResult() ?? array();
        }

        $result = parent::paginate(
            $event->getTable(),
            $event->getPage(),
            $event->getCriteria(),
            $event->getOptions(),
        );
        $event = new AfterPaginateEvent($event->getTable(), $result);

        $this->dispatch($event);

        return $event->getResult() ?? array();
    }

    public function count(string $table, array|string $criteria = null, array $options = null): int
    {
        $event = new CountEvent($table, $criteria, array(
            'root_event' => CountEvent::class,
        ) + ($options ?? array()));

        $this->dispatch($event);

        if ($event->isPropagationStopped()) {
            return $event->getResult() ?? 0;
        }

        $result = parent::count(
            $event->getTable(),
            $event->getCriteria(),
            $event->getOptions(),
        );
        $event = new AfterCountEvent($event->getTable(), $result);

        $this->dispatch($event);

        return $event->getResult() ?? 0;
    }

    public function select(string $table, array|string $criteria = null, array $options = null): array|null
    {
        $event = new SelectEvent($table, $criteria, $options, $options['root_event'] ?? null);

        $this->dispatch($event);

        if ($event->isPropagationStopped()) {
            return $event->getResult();
        }

        $result = parent::select(
            $event->getTable(),
            $event->getCriteria(),
            $event->getOptions(),
        );
        $event = new AfterSelectEvent($event->getTable(), $result, $options['root_event'] ?? null);

        $this->dispatch($event);

        return $event->getResult();
    }

    public function insert(string $table, array $data, array|string $options = null): bool|int|array|object|null
    {
        $event = new InsertEvent($table, $data, $options, $options['root_event'] ?? null);

        $this->dispatch($event);

        if ($event->isPropagationStopped()) {
            return $event->getResult();
        }

        $result = parent::insert(
            $event->getTable(),
            $event->getData(),
            $event->getOptions(),
        );
        $event = new AfterInsertEvent($event->getTable(), $result, $options['root_event'] ?? null);

        $this->dispatch($event);

        return $event->getResult();
    }

    public function update(string $table, array $data, array|string $criteria, array|bool|null $options = false): bool|int|array|object|null
    {
        $event = new UpdateEvent($table, $data, $criteria, $options, $options['root_event'] ?? null);

        $this->dispatch($event);

        if ($event->isPropagationStopped()) {
            return $event->getResult();
        }

        $result = parent::update(
            $event->getTable(),
            $event->getData(),
            $event->getCriteria(),
            $event->getOptions(),
        );
        $event = new AfterUpdateEvent($event->getTable(), $result, $options['root_event'] ?? null);

        $this->dispatch($event);

        return $event->getResult();
    }

    public function delete(string $table, array|string $criteria, array $options = null): bool|int
    {
        $event = new DeleteEvent($table, $criteria, array('root_event' => DeleteEvent::class) + ($options ?? array()));

        $this->dispatch($event);

        if ($event->isPropagationStopped()) {
            return $event->getResult();
        }

        $result = parent::delete(
            $event->getTable(),
            $event->getCriteria(),
            $event->getOptions(),
        );
        $event = new AfterDeleteEvent($event->getTable(), $result);

        $this->dispatch($event);

        return $event->getResult();
    }

    public function insertBatch(string $table, array $data, array|string $criteria = null, array|string $options = null): bool|int|array|null
    {
        $event = new InsertBatchEvent($table, $data, $criteria, $options);

        $this->dispatch($event);

        if ($event->isPropagationStopped()) {
            return $event->getResult();
        }

        $result = parent::insertBatch(
            $event->getTable(),
            $event->getData(),
            $event->getCriteria(),
            $event->getOptions(),
        );
        $event = new AfterInsertBatchEvent($event->getTable(), $result);

        $this->dispatch($event);

        return $event->getResult();
    }
}
