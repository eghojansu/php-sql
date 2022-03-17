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

    /** @var Di */
    private $di;

    /** @var Log */
    private $log;

    /** @var Dispatcher */
    private $dispatcher;

    protected function _before()
    {
        $this->di = new Di();
        $this->log = new Log(array('directory' => TEST_TMP));
        $this->dispatcher = new Dispatcher($this->di);
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

        $this->di->inject($this->db);
    }

    public function testEvents()
    {
        $this->db->listen('onInsert', function (InsertEvent $event) {
            $event->replaceData(array('name' => 'replaced'));
        });
        $this->db->listen('onAfterInsert', function (AfterInsertEvent $event) {
            $event->setResult($event->getResult() + array('saved' => true));
        });
        $this->db->listen(SelectEvent::class, function (SelectEvent $event, ListenableConnection $db) {
            $event->setCriteria(
                $db->getBuilder()->criteriaMerge(
                    $event->getCriteria(),
                    'hint IS NULL',
                ),
            );
        });
        $this->db->listen(AfterSelectEvent::class, function (AfterSelectEvent $event) {
            $result = $event->getResult();
            $result[0]['loaded'] = true;

            $event->setResult($result);
        });

        $expected = array(
            'id' => '1',
            'name' => 'replaced',
            'hint' => null,
            'saved' => true,
            'loaded' => true,
        );
        $actual = $this->db->insert('demo', array('name' => 'foo'), 'id');

        $this->assertEquals($expected, $actual);

        $this->db->unlisten('onInsert');

        $expected = array(
            'id' => '2',
            'name' => 'foo',
            'hint' => null,
            'saved' => true,
            'loaded' => true,
        );
        $actual = $this->db->insert('demo', array('name' => 'foo'), 'id');

        $this->assertEquals($expected, $actual);
    }
}
