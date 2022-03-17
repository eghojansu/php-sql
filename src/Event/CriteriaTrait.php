<?php

namespace Ekok\Sql\Event;

trait CriteriaTrait
{
    private $criteria = array();

    public function getCriteria(): array|string|null
    {
        return $this->criteria;
    }

    public function setCriteria(array|string|null $criteria): static
    {
        $this->criteria = $criteria ?? array();

        return $this;
    }
}
