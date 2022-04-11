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
        $this->dispatcher = $this->di->make(Dispatcher::class);
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

    public function testEvent()
    {
        $rootEventFlag = null;

        $this->db->listen('onInsert', function (InsertEvent $event) {
            $event->replaceData(array('name' => 'replaced'));
        });
        $this->db->listen('onAfterInsert', function (AfterInsertEvent $event) {
            $event->setResult($event->getResult() + array('saved' => true));
        });
        $this->db->listen('onUpdate', function (UpdateEvent $event) {
            $event->replaceData(array('name' => 'update replaced'));
        });
        $this->db->listen('onAfterUpdate', function (AfterUpdateEvent $event) {
            $event->setResult($event->getResult() + array('updated' => true));
        });
        $this->db->listen('onDelete', function (DeleteEvent $event, ListenableConnection $db) {
            $event->setCriteria(
                $db->getBuilder()->criteriaMerge(
                    $event->getCriteria(),
                    'hint IS NULL',
                ),
            );
        });
        $this->db->listen('onAfterDelete', function (AfterDeleteEvent $event) {
            $result = $event->getResult();
            $result[0]['deleted'] = true;

            $event->setResult($result);
        });
        $this->db->listen(SelectEvent::class, function (SelectEvent $event, ListenableConnection $db) use (&$rootEventFlag) {
            $rootEventFlag = $event->getRootEvent();

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
        $this->db->listen(CountEvent::class, function (CountEvent $event, ListenableConnection $db) {
            $event->setCriteria(
                $db->getBuilder()->criteriaMerge(
                    $event->getCriteria(),
                    'hint IS NULL',
                ),
            );
        });
        $this->db->listen(AfterCountEvent::class, function (AfterCountEvent $event) {
            $event->setResult($event->getResult() + 1);
        });
        $this->db->listen(PaginateEvent::class, function (PaginateEvent $event, ListenableConnection $db) {
            $event->setCriteria(
                $db->getBuilder()->criteriaMerge(
                    $event->getCriteria(),
                    'hint IS NULL',
                ),
            );
        });
        $this->db->listen(AfterPaginateEvent::class, function (AfterPaginateEvent $event) {
            $event->setResult($event->getResult() + array('paginated' => true));
        });
        $this->db->listen(InsertBatchEvent::class, function (InsertBatchEvent $event) {
            $event->setData(
                array_map(
                    static fn(array $row) => array('name' => $row['name'] . ' batch'),
                    $event->getData(),
                ),
            );
        });
        $this->db->listen(AfterInsertBatchEvent::class, function (AfterInsertBatchEvent $event) {
            $event->setResult($event->getResult() + 1);
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

        // update
        $expected = array(
            'id' => '2',
            'name' => 'update replaced',
            'hint' => null,
            'updated' => true,
            'loaded' => true,
        );
        $actual = $this->db->update('demo', array('name' => 'bar'), 'id = 2', true);

        $this->assertEquals($expected, $actual);

        // delete
        $expected = array(
            'id' => '2',
            'name' => 'update replaced',
            'hint' => null,
            'deleted' => true,
            'loaded' => true,
        );
        $actual = $this->db->delete('demo', 'id = 2', true);

        $this->assertEquals(array($expected), $actual);
        $this->assertSame(DeleteEvent::class, $rootEventFlag);

        // count
        $expected = 2;
        $actual = $this->db->count('demo');

        $this->assertSame($expected, $actual);

        // insert batch
        $expected = 3;
        $actual = $this->db->insertBatch('demo', array(
            array('name' => 'foo'),
            array('name' => 'bar'),
        ));

        $this->assertSame($expected, $actual);

        // paginate
        $actual = $this->db->paginate('demo', 1);

        $this->assertSame(true, $actual['paginated']);
        $this->assertSame(4, $actual['total']);
    }

    public function testPreventSelect()
    {
        $this->db->listen('onSelect', static fn(SelectEvent $event) => $event->stopPropagation()->setResult(array('not-selected' => true)));

        $expected = array('not-selected' => true);
        $actual = $this->db->select('demo', array());

        $this->assertEquals($expected, $actual);
    }

    public function testPreventInsert()
    {
        $this->db->listen('onInsert', static fn(InsertEvent $event) => $event->stopPropagation()->setResult(array('not-inserted' => true)));

        $expected = array('not-inserted' => true);
        $actual = $this->db->insert('demo', array());

        $this->assertEquals($expected, $actual);
    }

    public function testPreventUpdate()
    {
        $this->db->listen('onUpdate', static fn(UpdateEvent $event) => $event->stopPropagation()->setResult(array('not-updated' => true)));

        $expected = array('not-updated' => true);
        $actual = $this->db->update('demo', array(), '');

        $this->assertEquals($expected, $actual);
    }

    public function testPreventDelete()
    {
        $this->db->listen('onDelete', static fn(DeleteEvent $event) => $event->stopPropagation()->setResult(true));

        $expected = true;
        $actual = $this->db->delete('demo', '');

        $this->assertEquals($expected, $actual);
    }

    public function testPreventCount()
    {
        $this->db->listen('onCount', static fn(CountEvent $event) => $event->stopPropagation()->setResult(-1));

        $expected = -1;
        $actual = $this->db->count('demo');

        $this->assertEquals($expected, $actual);
    }

    public function testPreventInsertBatch()
    {
        $this->db->listen('onInsertBatch', static fn(InsertBatchEvent $event) => $event->stopPropagation()->setResult(-1));

        $expected = -1;
        $actual = $this->db->insertBatch('demo', array());

        $this->assertEquals($expected, $actual);
    }

    public function testPreventPaginate()
    {
        $this->db->listen('onPaginate', static fn(PaginateEvent $event) => $event->stopPropagation()->setResult(array(-1)));

        $expected = array(-1);
        $actual = $this->db->paginate('demo');

        $this->assertEquals($expected, $actual);
    }
}
