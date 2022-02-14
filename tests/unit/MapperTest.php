<?php

use Ekok\Logger\Log;
use Ekok\Sql\Mapper;
use Ekok\Sql\Connection;

class MapperTest extends \Codeception\Test\Unit
{
    /** @var Connection */
    private $db;

    /** @var Mapper */
    private $mapper;

    /** @var Log */
    private $log;

    protected function _before()
    {
        $this->log = new Log(array('directory' => TEST_TMP));
        $this->db = new Connection($this->log, 'sqlite::memory:', null, null, array(
            'scripts' => array(
                <<<'SQL'
CREATE TABLE "demo" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" VARCHAR(64) NOT NULL,
    "hint" VARCHAR(255) NULL
);
CREATE TABLE "composite_key" (
    "key1" VARCHAR(64) NOT NULL,
    "key2" VARCHAR(64) NOT NULL,
    "hint" VARCHAR(255) NULL
);
SQL
            ),
        ));
        $this->mapper = new Mapper($this->db, 'demo', 'id', array(
            'id' => 'int',
        ));
    }

    public function testUsage()
    {
        $foo = array('id' => 1, 'name' => 'foo', 'hint' => null);

        $this->assertSame('demo', $this->mapper->table());
        $this->assertSame(0, $this->mapper->countRow());
        $this->assertFalse($this->mapper->findOne()->valid());
        $this->assertTrue($this->mapper->fromArray(array('name' => 'foo'))->save());
        $this->assertTrue($this->mapper->findOne()->valid());
        $this->assertFalse($this->mapper->invalid());
        $this->assertCount(1, $this->mapper);
        $this->assertTrue(isset($this->mapper['name']));
        $this->assertSame('foo', $this->mapper['name']);
        $this->assertSame($foo, $this->mapper->toArray());
        $this->assertTrue($this->mapper->dry());
        $this->assertFalse($this->mapper->dirty());
        $this->assertSame(json_encode(array($foo)), json_encode($this->mapper));

        // updating
        $this->mapper['name'] = 'update';

        $this->assertFalse($this->mapper->dry());
        $this->assertTrue($this->mapper->dirty());
        $this->assertTrue($this->mapper->save());

        // insert new item
        $this->mapper->reset();
        $this->mapper['name'] = 'bar';
        $this->mapper['hint'] = 'to be removed';
        unset($this->mapper['hint']);

        $bar = array('id' => 2, 'name' => 'bar', 'hint' => null);

        $this->assertTrue($this->mapper->save());
        $this->assertSame(array($bar), $this->mapper->all());

        // confirmation
        $this->assertCount(2, $this->mapper->findAll());
        $this->assertSame(2, $this->mapper->countRow());
        $this->assertSame($bar, $this->mapper->find(2)->toArray());

        // manual queries
        $this->assertSame(1, $this->mapper->insert(array('name' => 'baz')));
        $this->assertSame(1, $this->mapper->update(array('name' => 'update'), 'id = 3'));
        $this->assertSame(1, $this->mapper->delete('id = 3'));
        $this->assertSame(0, $this->mapper->delete('id = 3'));
        $this->assertSame(2, $this->mapper->insertBatch(array(
            array('name' => 'four'),
            array('name' => 'five'),
        )));

        $page1 = $this->mapper->paginate();
        $page2 = $this->mapper->simplePaginate();
        $rows = $this->mapper->select();
        $row = $this->mapper->selectOne();

        $this->assertCount(4, $page1['subset']);
        $this->assertCount(4, $page2['subset']);
        $this->assertCount(4, $rows);
        $this->assertSame(1, $row['id']);
    }

    public function testUsageCompositKey()
    {
        $mapper = new Mapper($this->db, 'composite_key', array(
            'key1' => false,
            'key2' => false,
        ));
        $data = array(
            'key1' => 'foo',
            'key2' => 'bar',
            'hint' => 'hint',
        );

        $this->assertTrue($mapper->fromArray($data)->save());
        $this->assertCount(1, $mapper);
    }

    public function testUsageClass()
    {
        $this->db->getPdo()->exec(UserMap::tableSchema());

        $mapper = new UserMap($this->db);
        $foo = UserMap::generateRow('foo');
        $foo['name'] = 'update';
        $foo['value'] = 'update';
        $foo['active'] = true;
        $foo['prop_date'] = new \DateTime($foo['prop_date']);
        $foo['prop_datetime'] = new \DateTime($foo['prop_datetime']);
        unset($foo['ignored_column']);

        $this->assertSame('user_map', $mapper->table());
        $this->assertCount(0, $mapper);
        $this->assertTrue($mapper->fromArray($foo)->save());
        $this->assertCount(1, $mapper);

        $this->assertTrue(isset($mapper['value']));
        $this->assertTrue(isset($mapper['active']));
        $this->assertSame(null, $mapper['value']);

        $mapper['value'] = 'update';
        $mapper['name'] = 'update';

        $this->assertSame('update', $mapper['value']);
        $this->assertSame('update', $mapper['name']);
        $this->assertEquals($foo, $mapper->toArray());

        unset($mapper['value']);
        $this->assertSame(null, $mapper['value']);
    }

    public function testDrySaving()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('No data to be saved');

        $this->mapper->save();
    }

    public function testGetInvalidProperty()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('Column not exists: foo');

        $this->mapper['foo'];
    }

    public function testWriteCheck()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('This mapper is readonly');

        $this->db->getPdo()->exec(UserMap::tableSchema());

        $mapper = new UserMap($this->db, true);
        $mapper->fromArray(UserMap::generateRow('foo'))->save();
    }

    public function testColumnIgnoringCheck()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('Column access is forbidden: ignored_column');

        $this->db->getPdo()->exec(UserMap::tableSchema());

        $mapper = new UserMap($this->db);
        $mapper['ignored_column'] = 'set';
    }

    public function testColumnExcludedCheck()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('Column not exists: excluded');

        $this->db->getPdo()->exec(UserMap::tableSchema());

        $mapper = new UserMap($this->db);
        $mapper['excluded'] = 'set';
    }

    public function testNoKeysMapperCheck()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('This mapper has no keys');

        $mapper = new Mapper($this->db, 'demo');
        $mapper->find(1);
    }

    public function testInsufficientKeysCheck()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('Insufficient keys');

        $mapper = new Mapper($this->db, 'demo', array('foo', 'bar'));
        $mapper->find(1);
    }
}
