<?php

declare(strict_types=1);

namespace Ekok\Sql\Event;

trait OptionsTrait
{
    private $options;

    public function getOptions(): array|string|bool|null
    {
        return $this->options;
    }

    public function setOptions(array|string|bool|null $options): static
    {
        $this->options = $options;

        return $this;
    }
}
