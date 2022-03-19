<?php

use Ekok\Logger\Log;
use Ekok\Container\Di;
use Ekok\Sql\AuditableConnection;
use Ekok\EventDispatcher\Dispatcher;

class AuditableConnectionTest extends \Codeception\Test\Unit
{
    /** @var AuditableConnection */
    private $db;

    /** @var Di */
    private $di;

    /** @var Log */
    private $log;

    /** @var Dispatcher */
    private $dispatcher;

    /** @var string */
    private $now;

    /** @var string */
    private $blameTo;

    protected function _before()
    {
        $this->now = date('Y-m-d H:i:s');
        $this->blameTo = 'me';
        $this->di = new Di();
        $this->log = new Log(array('directory' => TEST_TMP, 'enabled' => true));
        $this->dispatcher = new Dispatcher($this->di);
        $this->db = new AuditableConnection(
            $this->dispatcher,
            $this->log,
            'sqlite::memory:',
            options: array(
                'timestamp' => $this->now,
                'blame_to' => $this->blameTo,
                'scripts' => array(
                <<<'SQL'
CREATE TABLE "demo" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" VARCHAR(64) NOT NULL,
    "hint" VARCHAR(255) NULL,
    "created_at" DATETIME NULL,
    "updated_at" DATETIME NULL,
    "deleted_at" DATETIME NULL,
    "created_by" DATETIME NULL,
    "updated_by" DATETIME NULL,
    "deleted_by" DATETIME NULL
)
SQL
                ),
            ),
        );
    }

    public function testUsage()
    {
        $expected = array(
            'id' => '1',
            'name' => 'foo',
            'hint' => null,
            'created_at' => $this->now,
            'updated_at' => $this->now,
            'deleted_at' => null,
            'created_by' => $this->blameTo,
            'updated_by' => $this->blameTo,
            'deleted_by' => null,
        );
        $actual = $this->db->insert('demo', array('name' => 'foo'), 'id');

        $this->assertEquals($expected, $actual);

        $expected['name'] = 'update';
        $actual = $this->db->update('demo', array('name' => 'update'), 'id=1', true);

        $this->assertEquals($expected, $actual);

        $expected['deleted_at'] = $this->now;
        $expected['deleted_by'] = $this->blameTo;
        $actual = $this->db->delete('demo', 'id=1', true);

        $this->assertEquals($expected, $actual);

        $this->assertEquals(array($expected), $this->db->selectWithTrashed('demo'));

        $expected = 1;
        $actual = $this->db->forceDelete('demo', 'id=1');

        $this->assertEquals($expected, $actual);

        $expected = array(
            'id' => '2',
            'name' => 'foo',
            'hint' => null,
            'created_at' => $this->now,
            'updated_at' => $this->now,
            'deleted_at' => null,
            'created_by' => $this->blameTo,
            'updated_by' => $this->blameTo,
            'deleted_by' => null,
        );
        $actual = $this->db->insertBatch('demo', array(
            array('name' => 'foo'),
        ), 'id=2');

        $this->assertEquals(array($expected), $actual);
    }

    public function testDisable()
    {
        $expected = array(
            'id' => '1',
            'name' => 'foo',
            'hint' => null,
            'created_at' => null,
            'updated_at' => null,
            'deleted_at' => null,
            'created_by' => null,
            'updated_by' => null,
            'deleted_by' => null,
        );
        $this->db->setEnable(false);

        $actual = $this->db->insert('demo', array('name' => 'foo'), 'id');

        $this->assertEquals($expected, $actual);
    }

    public function testWithCurrentDate()
    {
        $this->db->setOption('timestamp', null);

        $actual = $this->db->insert('demo', array('name' => 'foo'), 'id');

        $this->assertSame('foo', $actual['name']);
        $this->assertNotNull($actual['created_at']);
        $this->assertNotNull($actual['updated_at']);
        $this->assertNotNull($actual['created_by']);
        $this->assertNotNull($actual['updated_by']);
    }
}
