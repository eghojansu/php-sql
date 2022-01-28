<?php

namespace Ekok\Sql;

class CustomBuilder extends Builder
{
    protected $modifiers = array();

    public function addModifier(string $action, callable $modifier): static
    {
        $this->modifiers[strtolower($action)][] = $modifier;

        return $this;
    }

    public function hasModifier(string $action): bool
    {
        return isset($this->modifiers[strtolower($action)]);
    }

    public function mergeCriteria(array|string|null ...$criteria): array
    {
        return array_reduce(array_filter($criteria), function (array $criteria, $merge) {
            if (is_string($merge)) {
                $criteria[0] = $this->joinCriteria($criteria[0] ?? null, $merge);
            } elseif ($merge) {
                $criteria[0] = $this->joinCriteria($criteria[0] ?? null, $merge[0]);

                array_push($criteria, ...array_slice($merge, 1));
            }

            if (empty($criteria[0])) {
                $criteria = array();
            }

            return $criteria;
        }, array());
    }

    public function select(string $table, array|string|null $criteria = null, ?array $options = null): array
    {
        return parent::select(...$this->modify(__FUNCTION__, $table, $criteria, $options));
    }

    public function insert(string $table, array $data): array
    {
        return parent::insert(...$this->modify(__FUNCTION__, $table, $data));
    }

    public function update(string $table, array $data, array|string $criteria): array
    {
        return parent::update(...$this->modify(__FUNCTION__, $table, $data, $criteria));
    }

    public function delete(string $table, array|string $criteria): array
    {
        return parent::delete(...$this->modify(__FUNCTION__, $table, $criteria));
    }

    public function insertBatch(string $table, array $data): array
    {
        return parent::insertBatch(...$this->modify(__FUNCTION__, $table, $data));
    }

    protected function modify(string $action, ...$arguments): array
    {
        $modifiers = $this->modifiers[strtolower($action)] ?? array(static fn() => $arguments);
        $modified = array_reduce($modifiers, fn (array $arguments, callable $cb) => $cb(...array_merge($arguments, array($this))), $arguments);

        return $modified;
    }

    protected function joinCriteria(string|null $criteria, string|null $with): string
    {
        if (!$with) {
            return $criteria ?? '';
        }

        if ($criteria && $with && !preg_match('/^(and|or)/i', $with)) {
            return $criteria . ' AND (' . $with . ')';
        }

        return $criteria . $with;
    }
}
