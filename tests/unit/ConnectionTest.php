<?php

use Ekok\Logger\Log;
use Ekok\Sql\Mapper;
use Ekok\Sql\Connection;

class ConnectionTest extends \Codeception\Test\Unit
{
    /** @var Connection */
    private $db;

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
)
SQL
            ),
        ));
    }

    public function testDb()
    {
        $this->assertFalse($this->db->exists('unknown'));
        $this->assertTrue($this->db->exists('demo'));
        $this->assertNull($this->db->getName());
        $this->assertNotNull($this->db->getVersion());
        $this->assertEquals('sqlite', $this->db->getDriver());
    }

    public function testDbManipulation()
    {
        $this->assertCount(0, $this->db->select('demo'));
        $this->assertEquals(0, $this->db->count('demo'));
        $this->assertNull($this->db->selectOne('demo'));

        $this->assertEquals(1, $this->db->insert('demo', array('name' => 'foo', 'hint' => 'bar')));
        $this->assertEquals(2, $this->db->insertBatch('demo', array(
            array('name' => 'qux'),
            array('name' => 'quux'),
        )));
        $this->assertEquals(array('id' => 4, 'name' => 'load1', 'hint' => null), $this->db->insert('demo', array('name' => 'load1'), 'id'));
        $this->assertEquals(array('id' => 5, 'name' => 'load2', 'hint' => null), $this->db->insert('demo', array('name' => 'load2'), array('load' => 'id')));
        $this->assertEquals(array(
            array('id' => 6, 'name' => 'batch1', 'hint' => null),
            array('id' => 7, 'name' => 'batch2', 'hint' => null),
        ), $this->db->insertBatch('demo', array(
            array('name' => 'batch1'),
            array('name' => 'batch2'),
        ), 'id > 5'));

        $this->assertCount(7, $this->db->select('demo'));
        $this->assertEquals(7, $this->db->count('demo'));
        $this->assertEquals(array('id' => 1, 'name' => 'foo', 'hint' => 'bar'), $this->db->selectOne('demo'));

        $this->assertEquals(1, $this->db->update('demo', array('name' => 'qux update'), array('name = ?', 'qux')));
        $this->assertEquals(array('id' => 2, 'name' => 'qux update2', 'hint' => null), $this->db->update('demo', array('name' => 'qux update2'), 'id = 2', true));

        $this->assertEquals(4, $this->db->delete('demo', 'id > 3'));
        $this->assertEquals(3, $this->db->count('demo'));
    }

    public function testClone()
    {
        $this->expectExceptionMessage('Cloning Connection is prohibited');

        clone $this->db;
    }

    public function testErrorPdo()
    {
        $this->expectExceptionMessage('Unable to connect database');

        $db = new Connection($this->log, 'sqlite::memory:', null, null, array(
            'scripts' => array(
                <<<'SQL'
CREATE TABLE "demo" (
    "id" INTEGER NOT NULL AUTOINCREMENT,
    "name" VARCHAR(64) NOT NULL,
    "hint" VARCHAR(255) NULL
)
SQL
            ),
        ));
        $db->getPdo();
    }

    public function testInvalidQuery()
    {
        $this->log->enable();

        $this->assertFalse($this->db->insert('demo', array('foo' => 'bar')));
        $this->assertStringContainsString('General error: 1 table demo has no column named foo', $this->log->getLastLine());
    }

    public function testInvalidQueryOnDebug()
    {
        $this->log->enable();
        $this->db->setOption('debug', true);

        $this->assertFalse($this->db->insert('demo', array('foo' => 'bar')));
        $this->assertStringContainsString('General error: 1 table demo has no column named foo', $this->log->getLastLine());
    }

    public function testTransaction()
    {
        $expected = 3;
        $actual = $this->db->transact(
            0,
            static fn (Connection $db, $result) => $result + $db->insert('demo', array(
                'name' => 'foo',
            )),
            static fn (Connection $db, $result) => $result + $db->insert('demo', array(
                'name' => 'bar',
            )),
            static fn (Connection $db, $result) => $result + $db->insert('demo', array(
                'name' => 'baz',
            )),
        );

        $this->assertSame($expected, $actual);
    }

    /** @dataProvider simplePaginateProvider */
    public function testSimplePaginate($expected, ...$arguments)
    {
        $rowCount = $this->db->insertBatch('demo', array(
            array('name' => 'row1'),
            array('name' => 'row2'),
            array('name' => 'row3'),
            array('name' => 'row4'),
            array('name' => 'row5'),
        ));

        $this->assertEquals(5, $rowCount);

        $actual = $this->db->simplePaginate('demo', ...$arguments);
        $subset = $actual['subset'];
        unset($actual['subset']);

        $this->assertEquals($expected, $actual);
        $this->assertCount($expected['count'], $subset);
    }

    public function simplePaginateProvider()
    {
        return array(
            'default' => array(
                array('count' => 5, 'current_page' => 1, 'next_page' => 2, 'prev_page' => 0, 'per_page' => 20, 'total' => null, 'last_page' => 2, 'first' => 1, 'last' => 5, 'empty' => false),
            ),
            'filtering' => array(
                array('count' => 1, 'current_page' => 1, 'next_page' => 2, 'prev_page' => 0, 'per_page' => 20, 'total' => null, 'last_page' => 2, 'first' => 1, 'last' => 1, 'empty' => false),
                1,
                "name = 'row1'",
            ),
            'page 2' => array(
                array('count' => 0, 'current_page' => 2, 'next_page' => 3, 'prev_page' => 1, 'per_page' => 20, 'total' => null, 'last_page' => 3, 'first' => 21, 'last' => 21, 'empty' => false),
                2,
            ),
            'perpage = 2' => array(
                array('count' => 2, 'current_page' => 1, 'next_page' => 2, 'prev_page' => 0, 'per_page' => 2, 'total' => null, 'last_page' => 2, 'first' => 1, 'last' => 2, 'empty' => false),
                1,
                null,
                array('limit' => 2),
            ),
            'perpage = 2, page 3' => array(
                array('count' => 1, 'current_page' => 3, 'next_page' => 4, 'prev_page' => 2, 'per_page' => 2, 'total' => null, 'last_page' => 4, 'first' => 5, 'last' => 5, 'empty' => false),
                3,
                null,
                array('limit' => 2),
            ),
        );
    }

    /** @dataProvider paginateProvider */
    public function testPaginate($expected, ...$arguments)
    {
        $rowCount = $this->db->insertBatch('demo', array(
            array('name' => 'row1'),
            array('name' => 'row2'),
            array('name' => 'row3'),
            array('name' => 'row4'),
            array('name' => 'row5'),
        ));

        $this->assertEquals(5, $rowCount);

        $actual = $this->db->paginate('demo', ...$arguments);
        $subset = $actual['subset'];
        unset($actual['subset']);

        $this->assertEquals($expected, $actual);
        $this->assertCount($expected['count'], $subset);
    }

    public function paginateProvider()
    {
        return array(
            'default' => array(
                array('count' => 5, 'current_page' => 1, 'next_page' => 1, 'prev_page' => 0, 'last_page' => 1, 'total' => 5, 'first' => 1, 'last' => 5, 'per_page' => 20, 'empty' => false),
            ),
            'filtering' => array(
                array('count' => 1, 'current_page' => 1, 'next_page' => 1, 'prev_page' => 0, 'last_page' => 1, 'total' => 1, 'first' => 1, 'last' => 1, 'per_page' => 20, 'empty' => false),
                1,
                "name = 'row1'",
            ),
            'page 2' => array(
                array('count' => 0, 'current_page' => 2, 'next_page' => 1, 'prev_page' => 1, 'last_page' => 1, 'total' => 5, 'first' => 21, 'last' => 21, 'per_page' => 20, 'empty' => false),
                2,
            ),
            'perpage = 2' => array(
                array('count' => 2, 'current_page' => 1, 'next_page' => 2, 'prev_page' => 0, 'last_page' => 3, 'total' => 5, 'first' => 1, 'last' => 2, 'per_page' => 2, 'empty' => false),
                1,
                null,
                array('limit' => 2),
            ),
            'perpage = 2, page 3' => array(
                array('count' => 1, 'current_page' => 3, 'next_page' => 3, 'prev_page' => 2, 'last_page' => 3, 'total' => 5, 'first' => 5, 'last' => 5, 'per_page' => 2, 'empty' => false),
                3,
                null,
                array('limit' => 2),
            ),
        );
    }

    public function testOptions()
    {
        $expected = array(
            'pagination_size' => 20,
            'format_query' => null,
            'raw_identifier' => null,
            'table_prefix' => null,
            'quotes' => null,
            'debug' => false,
            'options' => null,
            'driver' => 'sqlite',
            'scripts' => array(),
        );
        $actual = $this->db->getOptions();
        $actual['scripts'] = array();

        $this->assertEquals($expected, $actual);
        $this->assertSame(30, $this->db->setOption('pagination_size', 30)->getOption('pagination_size'));
    }

    public function testMap()
    {
        $this->db->getPdo()->exec(UserMap::tableSchema());

        $this->db->addMap('demo');
        $this->db->addMap('user', UserMap::class);

        $demo = $this->db->map('demo');

        $this->assertInstanceOf(Mapper::class, $demo);
        $this->assertCount(0, $demo);
        $this->assertSame('demo', $demo->table());
        $this->assertTrue($demo->fromArray(array('name' => 'foo'))->save());

        $user = $this->db->map('user');

        $this->assertInstanceOf(UserMap::class, $user);
        $this->assertCount(0, $user);
        $this->assertSame('user_map', $user->table());
        $this->assertTrue($user->fromArray(UserMap::generateRow('foo'))->save());
        $this->assertTrue($user->valid());
        $this->assertCount(1, $user);
    }

    public function testUnregisteredMap()
    {
        $mapper = $this->db->map('demo');

        $this->assertSame('demo', $mapper->table());
    }

    /** @dataProvider stringifyProvider */
    public function testStringify(string $expected, string $sql, array $values = null)
    {
        $actual = $this->db->stringify($sql, $values);

        $this->assertSame($expected, $actual);
    }

    public function stringifyProvider()
    {
        return array(
            'std' => array(
                "SELECT * FROM table WHERE id = 999 AND name = 'FOO' AND active = true",
                'SELECT * FROM table WHERE id = ? AND name = ? AND active = ?',
                array(999, 'FOO', true),
            ),
            'no value' => array(
                "SELECT * FROM table",
                'SELECT * FROM table',
            ),
        );
    }

    public function testDeleteWithPreviousRowReturn()
    {
        $created = $this->db->insert('demo', array('name' => 'foo'), 'id');
        $deleted = $this->db->delete('demo', 'id=1', true);

        $this->assertSame(array($created), $deleted);
    }

    public function testSave()
    {
        $created = $this->db->save('demo', array('name' => 'foo'), 'id=1', array('load' => 'id'));
        $updated = $this->db->save('demo', array('name' => 'bar'), 'id=1', array('load' => 'id'));

        $this->assertNotEquals($created, $updated);
        $this->assertSame($updated['id'], $created['id']);
        $this->assertSame('foo', $created['name']);
        $this->assertSame('bar', $updated['name']);
    }

    public function testInsert()
    {
        $expected = 1;
        $actual = $this->db->insert('demo', array('name' => 'row 1'));

        $this->assertEquals($expected, $actual);

        $expected = array(
            'id' => 2,
            'name' => 'row 2',
            'hint' => null,
        );
        $actual = $this->db->insert('demo', array('name' => 'row 2'), array(
            'load' => array("name = 'row 2' and id = ?"),
        ));

        $this->assertEquals($expected, $actual);

        $expected = array(
            'id' => 3,
            'name' => 'row 3',
            'hint' => null,
        );
        $actual = $this->db->insert('demo', array('name' => 'row 3'), 'id');

        $this->assertEquals($expected, $actual);

        $expected = array(
            'id' => 50,
            'name' => 'row 50',
            'hint' => null,
        );
        $actual = $this->db->insert('demo', array('id' => 50, 'name' => 'row 50'), 'id');

        $this->assertEquals($expected, $actual);
    }

    public function testExec()
    {
        $this->assertSame(1, $this->db->exec("insert into demo (name) values ('foo')"));
        $this->assertSame(1, $this->db->execRaw("insert into demo (name) values ('bar')"));
    }

    public function testLogging()
    {
        $this->log->enable();
        $this->db->setOption('debug', true);

        $this->assertSame(1, $this->db->exec("insert into demo (name) values ('foo')"));
        $this->assertStringContainsString('Running query', $this->log->getLastLine());
    }
}
