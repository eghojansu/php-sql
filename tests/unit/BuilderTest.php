<?php

use Ekok\Sql\Builder;

class BuilderTest extends \Codeception\Test\Unit
{
    /** @var Builder */
    private $builder;

    protected function _before()
    {
        $this->builder = new Builder();
    }

    /** @dataProvider selectProvider */
    public function testSelect(array $expected, ...$arguments)
    {
        $actual = $this->builder->select(...$arguments);

        $this->assertEquals($expected, $actual);
    }

    public function selectProvider()
    {
        return array(
            'simple' => array(
                array('SELECT * FROM "demo"', array()),
                'demo',
            ),
            'standart' => array(
                array('SELECT * FROM "demo" WHERE id = ? ORDER BY "demo"."name" DESC LIMIT 1 OFFSET 2', array(1)),
                'demo',
                array('id = ?', 1),
                array(
                    'orders' => array('name' => 'desc'),
                    'offset' => 2,
                    'limit' => 1,
                ),
            ),
            'full' => array(
                array('SELECT "a"."name", "a"."hint", "b"."name" AS "b_name", "c"."name" FROM "demo" AS "a" JOIN demo b ON b.id = a.id left join demo c on c.id = b.id WHERE a.hint like ? GROUP BY a.name HAVING a.name like ? ORDER BY "a"."name" DESC LIMIT 10 OFFSET 1', array('%a%', '%a%')),
                'demo',
                array('a.hint like ?', '%a%'),
                array(
                    'alias' => 'a',
                    'columns' => array('name', 'hint', 'b_name' => 'b.name', 'c.name'),
                    'groups' => '"a.name',
                    'having' => array('a.name like ?', '%a%'),
                    'orders' => array('name' => 'desc'),
                    'offset' => 1,
                    'limit' => 10,
                    'joins' => array(
                        'demo b ON b.id = a.id',
                        'left join demo c on c.id = b.id',
                    ),
                ),
            ),
            'subquery' => array(
                array('SELECT * FROM (SELECT name FROM demo) AS "a"', array()),
                'SELECT name FROM demo',
                null,
                array('alias' => 'a', 'sub' => true),
            ),
        );
    }

    public function testSelectOffset()
    {
        $builder = new Builder('sqlsrv', null, '[]', '`');

        $expected = 'SELECT TOP 5 * FROM [demo] ORDER BY id';
        $this->assertEquals($expected, $builder->select('demo', null, array('limit' => 5, 'orders' => '`id'))[0]);

        $expected = 'SELECT * FROM [demo] ORDER BY [demo].[id] OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY';
        $this->assertEquals($expected, $builder->select('demo', null, array('limit' => 5, 'offset' => 10, 'orders' => 'id'))[0]);

        $this->expectException('LogicException');
        $this->expectExceptionMessage('Offsetting require column order');
        $builder->select('demo', null, array('limit' => 5));
    }

    public function testSelectSubQuery()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('Sub query needs an alias');
        $this->builder->select('demo', null, array('sub' => true));
    }

    /** @dataProvider insertProvider */
    public function testInsert(array $expected, ...$arguments)
    {
        $this->assertEquals($expected, $this->builder->insert(...$arguments));
    }

    public function insertProvider()
    {
        return array(
            'simple' => array(
                array(
                    'INSERT INTO "demo" ("name", "hint") VALUES (?, ?)',
                    array('foo', 'bar'),
                ),
                'demo',
                array('name' => 'foo', 'hint' => 'bar'),
            ),
        );
    }

    /** @dataProvider updateProvider */
    public function testUpdate(array $expected, ...$arguments)
    {
        $this->assertEquals($expected, $this->builder->update(...$arguments));
    }

    public function updateProvider()
    {
        return array(
            'simple' => array(
                array(
                    'UPDATE "demo" SET "name" = ?, "hint" = ? WHERE id = ?',
                    array('foo', 'bar', 1),
                ),
                'demo',
                array('name' => 'foo', 'hint' => 'bar'),
                array('id = ?', 1),
            ),
        );
    }

    /** @dataProvider deleteProvider */
    public function testDelete(array $expected, ...$arguments)
    {
        $this->assertEquals($expected, $this->builder->delete(...$arguments));
    }

    public function deleteProvider()
    {
        return array(
            'simple' => array(
                array(
                    'DELETE FROM "demo" WHERE id = ?',
                    array(1),
                ),
                'demo',
                array('id = ?', 1),
            ),
        );
    }

    /** @dataProvider insertBatchProvider */
    public function testInsertBatch(array $expected, ...$arguments)
    {
        $this->assertEquals($expected, $this->builder->insertBatch(...$arguments));
    }

    public function insertBatchProvider()
    {
        return array(
            'simple' => array(
                array(
                    'INSERT INTO "demo" ("name", "hint") VALUES (?, ?), (?, ?)',
                    array('foo', 'bar', 'baz', 'qux'),
                ),
                'demo',
                array(
                    array('name' => 'foo', 'hint' => 'bar'),
                    array('name' => 'baz', 'hint' => 'qux'),
                ),
            ),
        );
    }

    /** @dataProvider insertBatchExceptionProvider */
    public function testInsertBatchException(string $expected, ...$arguments)
    {
        $this->expectExceptionMessage($expected);

        $this->assertEquals($expected, $this->builder->insertBatch(...$arguments));
    }

    public function insertBatchExceptionProvider()
    {
        return array(
            'empty' => array(
                'No data to be inserted',
                'demo',
                array(
                    'invalid',
                ),
            ),
            'data not match' => array(
                'Invalid data at position: 1',
                'demo',
                array(
                    array('name' => 'foo', 'hint' => 'bar'),
                    array('name' => 'baz', 'with invalid data at this row'),
                ),
            ),
        );
    }

    public function testPlayingFormat()
    {
        $builder = new Builder(null, null, "'", null, true);
        $lf = "\n";

        $expected = "SELECT{$lf}*{$lf}FROM 'demo'";
        $this->assertEquals($expected, $builder->select('demo')[0]);

        $expected = "INSERT INTO 'demo'{$lf}('name',{$lf}'hint'){$lf}VALUES{$lf}(?, ?)";
        $this->assertEquals($expected, $builder->insert('demo', array('name' => 'foo', 'hint' => 'bar'))[0]);
    }

    public function testHelpers()
    {
        $builder = new Builder(null, 'prefix_', '`', '`');

        $this->assertSame('`foo`.`bar`', $builder->quote('foo.bar'));
        $this->assertSame(true, $builder->isRaw('`foo bar', $cut));
        $this->assertSame('foo bar', $cut);
        $this->assertSame('`foo bar', $builder->raw('foo bar'));
        $this->assertSame('table', $builder->table('`table'));
        $this->assertSame('prefix_table', $builder->table('table'));
        $this->assertSame('', $builder->criteriaJoin(null, null));
        $this->assertSame('foo', $builder->criteriaJoin(null, 'foo'));
        $this->assertSame('foo AND (bar)', $builder->criteriaJoin('foo', 'bar'));
        $this->assertSame('foo and bar', $builder->criteriaJoin('foo', 'and bar'));
        $this->assertSame(array(), $builder->criteriaMerge(array('')));
        $this->assertSame(array('foo AND (bar)'), $builder->criteriaMerge('foo', 'bar'));
        $this->assertSame(array('foo AND (bar)'), $builder->criteriaMerge(array('foo'), array('bar')));
        $this->assertSame(array('`foo` IN (?)', 'bar'), $builder->criteriaIn('foo', array('foo' => 'bar')));
        $this->assertSame(array('foo IN (?)', 'bar'), $builder->criteriaIn('`foo', array('foo' => 'bar')));
    }

    public function testCriteriaInEmpty()
    {
        $this->expectExceptionMessage('Data was empty');

        $this->builder->criteriaIn('foo', array());
    }
}
