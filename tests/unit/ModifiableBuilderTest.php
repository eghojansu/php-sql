<?php

use Ekok\Sql\Helper;
use Ekok\Sql\ModifiableBuilder;

class ModifiableBuilderTest extends \Codeception\Test\Unit
{
    /** @var ModifiableBuilder */
    private $builder;

    protected function _before()
    {
        $this->builder = new ModifiableBuilder(new Helper());
    }

    public function testSelect()
    {
        $this->builder->addModifier('select', static fn ($table, $criteria, $options, ModifiableBuilder $builder) => array($table, $builder->mergeCriteria($criteria, 'deleted_at is null'), $options));

        $expected = array('SELECT * FROM "demo" WHERE deleted_at is null', array());
        $actual = $this->builder->select('demo');

        $this->assertSame($expected, $actual);
        $this->assertSame(array(), $this->builder->mergeCriteria(array(''), array('')));
        $this->assertTrue($this->builder->hasModifier('select'));
        $this->assertFalse($this->builder->hasModifier('selects'));
    }

    public function testInsert()
    {
        $this->builder->addModifier('insert', static fn ($table, $data) => array($table, array_merge($data, array('created_at' => date('Y-m-d')))));

        $expected = array(
            'INSERT INTO "demo" ("name", "hint", "created_at") VALUES (?, ?, ?)',
            array('foo', 'bar', date('Y-m-d')),
        );
        $actual = $this->builder->insert('demo', array('name' => 'foo', 'hint' => 'bar'));

        $this->assertSame($expected, $actual);
    }

    public function testUpdate()
    {
        $this->builder->addModifier('update', static fn ($table, $data, $criteria) => array($table, array_merge($data, array('updated_at' => date('Y-m-d'))), $criteria));

        $expected = array(
            'UPDATE "demo" SET "name" = ?, "hint" = ?, "updated_at" = ? WHERE id = ?',
            array('foo', 'bar', date('Y-m-d'), 1),
        );
        $actual = $this->builder->update('demo', array('name' => 'foo', 'hint' => 'bar'), array('id = ?', 1));

        $this->assertSame($expected, $actual);
    }

    public function testDelete()
    {
        $this->builder->addModifier('delete', static fn ($table, $criteria, ModifiableBuilder $builder) => array($table, $builder->mergeCriteria($criteria, 'deleted_at is not null')));

        $expected = array(
            'DELETE FROM "demo" WHERE id = ? AND (deleted_at is not null)',
            array(1),
        );
        $actual = $this->builder->delete('demo', array('id = ?', 1));

        $this->assertSame($expected, $actual);
    }

    public function testInsertBatch()
    {
        $this->builder->addModifier('insertBatch', static fn ($table, $data) => array($table, array_map(fn ($row) => array_merge($row, array('date' => date('Y-m-d'))), $data)));

        $expected = array(
            'INSERT INTO "demo" ("one", "date") VALUES (?, ?), (?, ?)',
            array(1, date('Y-m-d'), 2, date('Y-m-d')),
        );
        $actual = $this->builder->insertBatch('demo', array(
            array('one' => 1),
            array('one' => 2),
        ));

        $this->assertSame($expected, $actual);
    }
}
