<?php

declare(strict_types=1);

namespace Ekok\Sql;

class AuditableConnection extends ListenableConnection
{
    public function isEnabled(): bool
    {
        return $this->getOption('enabled') ?? true;
    }

    public function setEnabled(bool $enabled): static
    {
        $this->setOption('enabled', $enabled);

        return $this;
    }

    public function getCreatedAtColumn(): string
    {
        return $this->getOption('created_at') ?? 'created_at';
    }

    public function getUpdatedAtColumn(): string
    {
        return $this->getOption('updated_at') ?? 'updated_at';
    }

    public function getDeletedAtColumn(): string
    {
        return $this->getOption('deleted_at') ?? 'deleted_at';
    }

    public function getCreatedByColumn(): string
    {
        return $this->getOption('created_by') ?? ($this->getBlameToIdentifier() ? 'created_by' : '');
    }

    public function getUpdatedByColumn(): string
    {
        return $this->getOption('updated_by') ?? ($this->getBlameToIdentifier() ? 'updated_by' : '');
    }

    public function getDeletedByColumn(): string
    {
        return $this->getOption('deleted_by') ?? ($this->getBlameToIdentifier() ? 'deleted_by' : '');
    }

    public function getBlameToIdentifier(): string|int
    {
        return $this->getOption('blame_to') ?? '';
    }

    public function getTimestamp(): string
    {
        return $this->getOption('timestamp') ?? date($this->getTimeFormat());
    }

    public function getTimeFormat(): string
    {
        return $this->getOption('time_format') ?? 'Y-m-d H:i:s';
    }

    public function selectWithTrashed(string $table, array|string $criteria = null, array $options = null): array|null
    {
        return parent::select($table, $criteria, $options);
    }

    public function select(string $table, array|string $criteria = null, array $options = null): array|null
    {
        $useCriteria = $criteria;

        if ($this->isEnabled() && ($options['ignore_deleted'] ?? true) && ($deleted = $this->getDeletedAtColumn())) {
            $builder = $this->getBuilder();
            $useCriteria = $builder->criteriaMerge(
                $useCriteria,
                $deleted . ' IS NULL',
            );
        }

        return parent::select($table, $useCriteria, $options);
    }

    public function insert(string $table, array $data, array|string $options = null): bool|int|array|object|null
    {
        $useData = $data;

        if (
            $this->isEnabled()
            && ($createdAt = $this->getCreatedAtColumn())
            && empty($useData[$createdAt])
        ) {
            $useData[$createdAt] = $this->getTimestamp();

            if ($updatedAt = $this->getUpdatedAtColumn()) {
                $useData[$updatedAt] = $useData[$createdAt];
            }
        }

        if (
            $this->isEnabled()
            && ($createdBy = $this->getCreatedByColumn())
            && empty($useData[$createdBy])
            && ($by = $this->getBlameToIdentifier())
        ) {
            $useData[$createdBy] = $by;

            if ($updatedBy = $this->getUpdatedByColumn()) {
                $useData[$updatedBy] = $useData[$createdBy];
            }
        }

        return parent::insert($table, $useData, $options);
    }

    public function update(string $table, array $data, array|string $criteria, array|bool|null $options = false): bool|int|array|object|null
    {
        $useData = $data;

        if (
            $this->isEnabled()
            && ($updatedAt = $this->getUpdatedAtColumn())
        ) {
            $useData[$updatedAt] = $this->getTimestamp();
        }

        if (
            $this->isEnabled()
            && ($updatedBy = $this->getUpdatedByColumn())
            && ($by = $this->getBlameToIdentifier())
        ) {
            $useData[$updatedBy] = $by;
        }

        return parent::update($table, $useData, $criteria, $options);
    }

    public function delete(string $table, array|string $criteria, array|bool|null $options = null): bool|int|array|object|null
    {
        $update = $this->isEnabled() && !($options['force_delete'] ?? false) ? array() : null;

        if (null !== $update && ($deletedAt = $this->getDeletedAtColumn())) {
            $update[$deletedAt] = $this->getTimestamp();
        }

        if (
            null !== $update
            && ($deletedBy = $this->getDeletedByColumn())
            && ($by = $this->getBlameToIdentifier())
        ) {
            $update[$deletedBy] = $by;
        }

        if ($update) {
            $options_ = is_bool($options) ? array('load' => $options) : (array) $options;
            $options_['ignore_deleted'] = false;

            return parent::update($table, $update, $criteria, $options_);
        }

        return parent::delete($table, $criteria, $options);
    }

    public function forceDelete(string $table, array|string $criteria, array|bool|null $options = null): bool|int|array|object
    {
        $options_ = is_bool($options) ? array('load' => $options) : (array) $options;
        $options_['force_delete'] = true;

        return self::delete($table, $criteria, $options_);
    }

    public function insertBatch(string $table, array $data, array|string $criteria = null, array|string $options = null): bool|int|array|null
    {
        $useData = $data;
        $add = $this->isEnabled() ? array() : null;

        if (null !== $add && ($createdAt = $this->getCreatedAtColumn())) {
            $add[$createdAt] = $this->getTimestamp();

            if ($updatedAt = $this->getUpdatedAtColumn()) {
                $add[$updatedAt] = $add[$createdAt];
            }
        }

        if (
            null !== $add
            && ($createdBy = $this->getCreatedByColumn())
            && ($by = $this->getBlameToIdentifier())
        ) {
            $add[$createdBy] = $by;

            if ($updatedBy = $this->getUpdatedByColumn()) {
                $add[$updatedBy] = $add[$createdBy];
            }
        }

        if ($add) {
            $useData = array_map(static fn(array $row) => $row + $add, $useData);
        }

        return parent::insertBatch($table, $useData, $criteria, $options);
    }
}
