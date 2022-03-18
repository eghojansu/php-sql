<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

trait DataTrait
{
    private $data = array();

    public function getData(): array
    {
        return $this->data;
    }

    public function replaceData(array $data): static
    {
        return $this->setData(array_replace($this->data, $data));
    }

    public function setData(array|null $data): static
    {
        $this->data = $data ?? array();

        return $this;
    }
}
