<?php

use Ekok\Logger\Log;
use Ekok\Container\Di;
use Ekok\Sql\ListenableConnection;
use Ekok\Sql\Event\AfterCount as AfterCountEvent;
use Ekok\Sql\Event\AfterPaginate as AfterPaginateEvent;
use Ekok\Sql\Event\AfterSelect as AfterSelectEvent;
use Ekok\Sql\Event\AfterInsert as AfterInsertEvent;
use Ekok\Sql\Event\AfterUpdate as AfterUpdateEvent;
use Ekok\Sql\Event\AfterDelete as AfterDeleteEvent;
use Ekok\Sql\Event\AfterInsertBatch as AfterInsertBatchEvent;
use Ekok\Sql\Event\Count as CountEvent;
use Ekok\Sql\Event\Paginate as PaginateEvent;
use Ekok\Sql\Event\Select as SelectEvent;
use Ekok\Sql\Event\Insert as InsertEvent;
use Ekok\Sql\Event\Update as UpdateEvent;
use Ekok\Sql\Event\Delete as DeleteEvent;
use Ekok\Sql\Event\InsertBatch as InsertBatchEvent;
use Ekok\EventDispatcher\Dispatcher;

class ListenableConnectionTest extends \Codeception\Test\Unit
{
    /** @var ListenableConnection */
    private $db;

    /** @var Log */
    private $log;

    /** @var Dispatcher */
    private $dispatcher;

    protected function _before()
    {
        $this->log = new Log(array('directory' => TEST_TMP));
        $this->dispatcher = new Dispatcher(new Di());
        $this->db = new ListenableConnection(
            $this->dispatcher,
            $this->log,
            'sqlite::memory:',
            options: array(
                'scripts' => array(
                <<<'SQL'
CREATE TABLE "demo" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" VARCHAR(64) NOT NULL,
    "hint" VARCHAR(255) NULL
)
SQL
                ),
            ),
        );
    }

    public function testEvents()
    {
        $this->db->on(InsertEvent::class, function (InsertEvent $event) {
            $event->replaceData(array('name' => 'replaced'));
        });
        $this->db->one(AfterInsertEvent::class, function (AfterInsertEvent $event) {
            $event->setResult($event->getResult() + array('added' => true));
        });

        $expected = array(
            'id' => '1',
            'name' => 'replaced',
            'hint' => null,
            'added' => true,
        );
        $actual = $this->db->insert('demo', array('name' => 'foo'), 'id');

        $this->assertSame($expected, $actual);
    }
}
