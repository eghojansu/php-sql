<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

class Paginate extends Select
{
    private $page;

    public function __construct(
        string $table,
        int $page,
        string|array $criteria = null,
        array $options = null,
    ) {
        parent::__construct($table, $criteria, $options);

        $this->setPage($page);
    }

    public function getPage(): int
    {
        return $this->page;
    }

    public function setPage(int $page): static
    {
        $this->page = $page;

        return $this;
    }
}
