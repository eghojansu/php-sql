<?php

use Ekok\Sql\Helper;

class HelperTest extends \Codeception\Test\Unit
{
    /** @var Helper */
    private $helper;

    protected function _before()
    {
        $this->helper = new Helper('`', '`', 'prefix_');
    }

    public function testFunctionality()
    {
        $this->assertSame('`foo`.`bar`', $this->helper->quote('foo.bar'));
        $this->assertSame(true, $this->helper->isRaw('`foo bar', $cut));
        $this->assertSame('foo bar', $cut);
        $this->assertSame('`foo bar', $this->helper->raw('foo bar'));
        $this->assertSame('table', $this->helper->table('`table'));
        $this->assertSame('prefix_table', $this->helper->table('table'));
        $this->assertSame('', $this->helper->joinCriteria(null, null));
        $this->assertSame('foo', $this->helper->joinCriteria(null, 'foo'));
        $this->assertSame('foo AND (bar)', $this->helper->joinCriteria('foo', 'bar'));
        $this->assertSame('foo and bar', $this->helper->joinCriteria('foo', 'and bar'));
        $this->assertSame(array(), $this->helper->mergeCriteria(array('')));
        $this->assertSame(array('foo AND (bar)'), $this->helper->mergeCriteria('foo', 'bar'));
        $this->assertSame(array('foo AND (bar)'), $this->helper->mergeCriteria(array('foo'), array('bar')));
    }
}
