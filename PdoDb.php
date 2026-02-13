<?php
declare(strict_types=1);

/**
 * Future-Proof PDO Database Core (Single File)
 *
 * What this is:
 * - A production-focused PDO execution core + lightweight query builder (MysqliDb-style ergonomics).
 * - Prepared statements only (builder + raw queries).
 * - Safe identifier quoting and strict validation for tables/columns.
 * - Optional, zero-dependency hooks for logging/metrics/auditing/retry/health checks.
 *
 * What this is not:
 * - ORM / Active Record.
 * - SQL DSL or regex-based parser.
 * - Framework or container integration.
 *
 * Key stability choices:
 * - Deterministic builder state that is reset after each execution.
 * - No global mutable state and no hidden singletons.
 * - Lazy connection initialization.
 */

if (!class_exists('DbException', false)) {
    final class DbException extends \RuntimeException
    {
        /** @var array<string,mixed> */
        private array $context;

        /**
         * @param array<string,mixed> $context
         */
        public function __construct(string $message, array $context = [], ?\Throwable $previous = null)
        {
            parent::__construct($message, 0, $previous);
            $this->context = $context;
        }

        /**
         * @return array<string,mixed>
         */
        public function context(): array
        {
            return $this->context;
        }
    }
}

if (!class_exists('DbConfig', false)) {
    final class DbConfig
    {
        public readonly string $dsn;
        public readonly string $username;
        public readonly string $password;
        public readonly string $charset;
        public readonly int $timeoutSeconds;
        public readonly bool $persistent;
        /** @var array<int|string, mixed> */
        public readonly array $pdoOptions;

        /**
         * @param array<int|string, mixed> $pdoOptions
         */
        public function __construct(
            string $dsn,
            string $username = '',
            string $password = '',
            string $charset = 'utf8mb4',
            int $timeoutSeconds = 5,
            bool $persistent = false,
            array $pdoOptions = []
        ) {
            $this->dsn = $dsn;
            $this->username = $username;
            $this->password = $password;
            $this->charset = $charset;
            $this->timeoutSeconds = max(0, $timeoutSeconds);
            $this->persistent = $persistent;
            $this->pdoOptions = $pdoOptions;
        }

        /**
         * @param array<string,mixed> $config
         */
        public static function fromArray(array $config): self
        {
            $dsn = (string)($config['dsn'] ?? '');
            if ($dsn === '') {
                throw new \InvalidArgumentException('Connection config requires "dsn"');
            }

            return new self(
                $dsn,
                (string)($config['username'] ?? $config['user'] ?? ''),
                (string)($config['password'] ?? $config['pass'] ?? ''),
                (string)($config['charset'] ?? 'utf8mb4'),
                (int)($config['timeout'] ?? $config['timeout_seconds'] ?? 5),
                (bool)($config['persistent'] ?? false),
                is_array($config['options'] ?? null) ? (array)$config['options'] : []
            );
        }

        /**
         * @return array<int, mixed>
         */
        public function toPdoOptions(): array
        {
            $opts = $this->pdoOptions;

            $opts[\PDO::ATTR_ERRMODE] = $opts[\PDO::ATTR_ERRMODE] ?? \PDO::ERRMODE_EXCEPTION;
            $opts[\PDO::ATTR_DEFAULT_FETCH_MODE] = $opts[\PDO::ATTR_DEFAULT_FETCH_MODE] ?? \PDO::FETCH_ASSOC;
            $opts[\PDO::ATTR_EMULATE_PREPARES] = $opts[\PDO::ATTR_EMULATE_PREPARES] ?? false;

            if ($this->persistent) {
                $opts[\PDO::ATTR_PERSISTENT] = true;
            }

            if ($this->timeoutSeconds > 0) {
                $opts[\PDO::ATTR_TIMEOUT] = $this->timeoutSeconds;
            }

            return $opts;
        }

        /**
         * @return array<string,mixed>
         */
        public function safeForLogs(): array
        {
            return [
                'dsn' => DbLogUtil::sanitizeDsn($this->dsn),
                'username' => $this->username,
                'charset' => $this->charset,
                'timeout_seconds' => $this->timeoutSeconds,
                'persistent' => $this->persistent,
            ];
        }
    }
}

if (!class_exists('DbHooks', false)) {
    final class DbHooks
    {
        /** @var null|callable(array<string,mixed>):void */
        public $beforeQuery = null;
        /** @var null|callable(array<string,mixed>):void */
        public $afterQuery = null;
        /** @var null|callable(array<string,mixed>):void */
        public $onError = null;
        /** @var null|callable(array<string,mixed>):void */
        public $onConnect = null;
        /** @var null|callable(array<string,mixed>):mixed */
        public $retryDecider = null;
        /** @var null|callable(array<string,mixed>):mixed */
        public $healthCheck = null;
    }
}

if (!class_exists('DbLogUtil', false)) {
    final class DbLogUtil
    {
        public static function sanitizeDsn(string $dsn): string
        {
            $parts = explode(';', $dsn);
            $safe = [];
            foreach ($parts as $part) {
                $p = trim($part);
                if ($p === '') {
                    continue;
                }
                [$k, $v] = array_pad(explode('=', $p, 2), 2, '');
                $k = strtolower(trim($k));
                if ($k === 'password' || $k === 'pwd') {
                    $safe[] = $k . '=***';
                } else {
                    $safe[] = $p;
                }
            }
            return implode(';', $safe);
        }

        /**
         * @param list<mixed> $params
         * @return list<array{type:string,value:string}>
         */
        public static function sanitizeParams(array $params, int $maxLen = 128): array
        {
            $out = [];
            foreach ($params as $v) {
                $type = get_debug_type($v);
                $str = self::stringify($v);
                if (strlen($str) > $maxLen) {
                    $str = substr($str, 0, $maxLen) . '…';
                }
                $out[] = ['type' => $type, 'value' => $str];
            }
            return $out;
        }

        private static function stringify(mixed $v): string
        {
            if ($v === null) {
                return 'null';
            }
            if (is_bool($v)) {
                return $v ? 'true' : 'false';
            }
            if (is_int($v) || is_float($v)) {
                return (string)$v;
            }
            if (is_string($v)) {
                return $v;
            }
            if (is_array($v)) {
                return 'array(' . count($v) . ')';
            }
            if ($v instanceof \Stringable) {
                return (string)$v;
            }
            return (string)get_debug_type($v);
        }
    }
}

if (!class_exists('DbIdentifier', false)) {
    final class DbIdentifier
    {
        private string $driver;
        private string $qOpen;
        private string $qClose;

        public function __construct(string $pdoDriverName)
        {
            $this->driver = strtolower($pdoDriverName);
            if ($this->driver === 'mysql') {
                $this->qOpen = '`';
                $this->qClose = '`';
            } else {
                $this->qOpen = '"';
                $this->qClose = '"';
            }
        }

        public function driver(): string
        {
            return $this->driver;
        }

        public function supportsSavepoints(): bool
        {
            return in_array($this->driver, ['mysql', 'pgsql', 'sqlite'], true);
        }

        public function table(string $name, ?string $alias = null): string
        {
            $quoted = $this->identifierWithDots($name);
            if ($alias === null || $alias === '') {
                return $quoted;
            }
            return $quoted . ' AS ' . $this->identifier($alias);
        }

        public function column(string $name): string
        {
            return $this->identifierWithDotsOrStar($name);
        }

        /**
         * Quotes a simple identifier without dots.
         */
        public function identifier(string $name): string
        {
            $name = trim($name);
            if (!$this->isSafeIdent($name)) {
                throw new DbException('Unsafe identifier: ' . $name);
            }
            return $this->qOpen . $name . $this->qClose;
        }

        public function identifierWithDots(string $name): string
        {
            $name = trim($name);
            if ($name === '') {
                throw new DbException('Empty identifier');
            }
            $parts = explode('.', $name);
            $out = [];
            foreach ($parts as $p) {
                $p = trim($p);
                if (!$this->isSafeIdent($p)) {
                    throw new DbException('Unsafe identifier segment: ' . $p);
                }
                $out[] = $this->qOpen . $p . $this->qClose;
            }
            return implode('.', $out);
        }

        public function identifierWithDotsOrStar(string $name): string
        {
            $name = trim($name);
            if ($name === '*') {
                return '*';
            }
            if (str_ends_with($name, '.*')) {
                $base = substr($name, 0, -2);
                return $this->identifierWithDots($base) . '.*';
            }
            return $this->identifierWithDots($name);
        }

        private function isSafeIdent(string $s): bool
        {
            return (bool)preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $s);
        }
    }
}

if (!class_exists('DbStatementCache', false)) {
    final class DbStatementCache
    {
        /** @var array<string,\PDOStatement> */
        private array $cache = [];
        /** @var list<string> */
        private array $order = [];
        private int $max;

        public function __construct(int $max = 128)
        {
            $this->max = max(0, $max);
        }

        public function get(\PDO $pdo, string $sql): \PDOStatement
        {
            if ($this->max === 0) {
                return $pdo->prepare($sql);
            }

            if (isset($this->cache[$sql])) {
                return $this->cache[$sql];
            }

            $stmt = $pdo->prepare($sql);
            $this->cache[$sql] = $stmt;
            $this->order[] = $sql;

            if (count($this->order) > $this->max) {
                $evict = array_shift($this->order);
                if ($evict !== null) {
                    unset($this->cache[$evict]);
                }
            }

            return $stmt;
        }
    }
}

if (!class_exists('DbConnectionManager', false)) {
    final class DbConnectionManager
    {
        /** @var array<string,DbConfig> */
        private array $configs = [];
        /** @var array<string,\PDO> */
        private array $pdos = [];
        /** @var array<string,DbIdentifier> */
        private array $identifiers = [];
        /** @var array<string,DbStatementCache> */
        private array $stmtCaches = [];
        /** @var array<string,int> */
        private array $txDepth = [];

        public function __construct(private ?DbHooks $hooks = null) {}

        public function add(string $name, DbConfig $config): void
        {
            $name = trim($name);
            if ($name === '') {
                throw new \InvalidArgumentException('Connection name cannot be empty');
            }
            $this->configs[$name] = $config;
        }

        /**
         * @param array<string,DbConfig|array<string,mixed>> $connections
         */
        public static function fromArray(array $connections, ?DbHooks $hooks = null): self
        {
            $m = new self($hooks);
            foreach ($connections as $name => $cfg) {
                if ($cfg instanceof DbConfig) {
                    $m->add((string)$name, $cfg);
                } else {
                    $m->add((string)$name, DbConfig::fromArray((array)$cfg));
                }
            }
            return $m;
        }

        public function pdo(string $name): \PDO
        {
            if (isset($this->pdos[$name])) {
                return $this->pdos[$name];
            }
            if (!isset($this->configs[$name])) {
                throw new DbException('Unknown connection: ' . $name);
            }

            $cfg = $this->configs[$name];
            $pdo = new \PDO($cfg->dsn, $cfg->username, $cfg->password, $cfg->toPdoOptions());
            $this->applySessionSettings($pdo, $cfg);

            $this->pdos[$name] = $pdo;
            $this->identifiers[$name] = new DbIdentifier($pdo->getAttribute(\PDO::ATTR_DRIVER_NAME));
            $this->stmtCaches[$name] = new DbStatementCache(128);
            $this->txDepth[$name] = 0;

            if ($this->hooks?->onConnect !== null) {
                ($this->hooks->onConnect)([
                    'connection' => $name,
                    'config' => $cfg->safeForLogs(),
                    'driver' => $this->identifiers[$name]->driver(),
                ]);
            }

            return $pdo;
        }

        public function ident(string $name): DbIdentifier
        {
            $this->pdo($name);
            return $this->identifiers[$name];
        }

        public function stmt(string $name, string $sql): \PDOStatement
        {
            $pdo = $this->pdo($name);
            return $this->stmtCaches[$name]->get($pdo, $sql);
        }

        public function txDepth(string $name): int
        {
            $this->pdo($name);
            return $this->txDepth[$name] ?? 0;
        }

        public function setTxDepth(string $name, int $depth): void
        {
            $this->txDepth[$name] = max(0, $depth);
        }

        private function applySessionSettings(\PDO $pdo, DbConfig $cfg): void
        {
            $driver = strtolower((string)$pdo->getAttribute(\PDO::ATTR_DRIVER_NAME));
            if ($driver === 'mysql' && $cfg->charset !== '') {
                $pdo->exec('SET NAMES ' . $cfg->charset);
            }
        }
    }
}

if (!class_exists('DbQueryState', false)) {
    final class DbQueryState
    {
        /** @var list<array{bool:string,col:string,op:string,val:mixed}> */
        public array $where = [];
        /** @var list<array{bool:string,col:string,op:string,val:mixed}> */
        public array $having = [];
        /** @var list<array{type:string,table:string,alias:?string,left:string,op:string,right:string}> */
        public array $joins = [];
        /** @var list<string> */
        public array $groupBy = [];
        /** @var list<array{col:string,dir:string}> */
        public array $orderBy = [];
        public ?int $limit = null;
        public ?int $offset = null;

        public function reset(): void
        {
            $this->where = [];
            $this->having = [];
            $this->joins = [];
            $this->groupBy = [];
            $this->orderBy = [];
            $this->limit = null;
            $this->offset = null;
        }
    }
}

if (!class_exists('PdoDb', false)) {
    final class PdoDb
    {
        private string $connection;
        private DbQueryState $state;
        private ?DbHooks $hooks;

        private bool $debug = false;
        private int $debugMaxLog = 200;
        /** @var list<array<string,mixed>> */
        private array $queryLog = [];
        /** @var null|array<string,mixed> */
        private ?array $lastError = null;
        /** @var null|array<string,mixed> */
        private ?array $lastQuery = null;

        private string $errorMode = 'exception';
        private string $fetchMode = 'array';
        private int $jsonFlags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES;

        public function __construct(private DbConnectionManager $connections, string $connection = 'default', ?DbHooks $hooks = null)
        {
            $this->connection = $connection;
            $this->hooks = $hooks;
            $this->state = new DbQueryState();
        }

        /**
         * @param array<string,DbConfig|array<string,mixed>> $connections
         */
        public static function create(array $connections, string $default = 'default', ?DbHooks $hooks = null): self
        {
            $mgr = DbConnectionManager::fromArray($connections, $hooks);
            return new self($mgr, $default, $hooks);
        }

        public function withConnection(string $name): self
        {
            $clone = clone $this;
            $clone->connection = $name;
            $clone->state = new DbQueryState();
            return $clone;
        }

        public function debug(bool $enabled = true, int $maxLog = 200): self
        {
            $this->debug = $enabled;
            $this->debugMaxLog = max(0, $maxLog);
            return $this;
        }

        public function errorMode(string $mode): self
        {
            $mode = strtolower(trim($mode));
            if (!in_array($mode, ['exception', 'safe'], true)) {
                throw new \InvalidArgumentException('errorMode must be "exception" or "safe"');
            }
            $this->errorMode = $mode;
            return $this;
        }

        public function asArray(): self
        {
            $this->fetchMode = 'array';
            return $this;
        }

        public function asObject(): self
        {
            $this->fetchMode = 'object';
            return $this;
        }

        public function asJson(int $flags = null): self
        {
            $this->fetchMode = 'json';
            if ($flags !== null) {
                $this->jsonFlags = $flags;
            }
            return $this;
        }

        /**
         * @return list<array<string,mixed>>
         */
        public function queryLog(): array
        {
            return $this->queryLog;
        }

        /**
         * @return null|array<string,mixed>
         */
        public function lastError(): ?array
        {
            return $this->lastError;
        }

        /**
         * @return null|array<string,mixed>
         */
        public function lastQuery(): ?array
        {
            return $this->lastQuery;
        }

        public function where(string $column, mixed $value, string $operator = '='): self
        {
            $this->state->where[] = ['bool' => 'AND', 'col' => $column, 'op' => $operator, 'val' => $value];
            return $this;
        }

        public function orWhere(string $column, mixed $value, string $operator = '='): self
        {
            $this->state->where[] = ['bool' => 'OR', 'col' => $column, 'op' => $operator, 'val' => $value];
            return $this;
        }

        public function having(string $column, mixed $value, string $operator = '='): self
        {
            $this->state->having[] = ['bool' => 'AND', 'col' => $column, 'op' => $operator, 'val' => $value];
            return $this;
        }

        public function orHaving(string $column, mixed $value, string $operator = '='): self
        {
            $this->state->having[] = ['bool' => 'OR', 'col' => $column, 'op' => $operator, 'val' => $value];
            return $this;
        }

        public function join(string $table, string $on, string $type = 'LEFT', ?string $alias = null): self
        {
            $type = strtoupper(trim($type));
            if (!in_array($type, ['INNER', 'LEFT', 'RIGHT'], true)) {
                throw new \InvalidArgumentException('Unsupported join type: ' . $type);
            }

            [$left, $op, $right] = DbSqlUtil::parseSimpleBinaryExpr($on);
            $this->state->joins[] = ['type' => $type, 'table' => $table, 'alias' => $alias, 'left' => $left, 'op' => $op, 'right' => $right];
            return $this;
        }

        public function groupBy(string|array $columns): self
        {
            $cols = is_array($columns) ? $columns : [$columns];
            foreach ($cols as $c) {
                $c = trim((string)$c);
                if ($c !== '') {
                    $this->state->groupBy[] = $c;
                }
            }
            return $this;
        }

        public function orderBy(string $column, string $direction = 'ASC'): self
        {
            $dir = strtoupper(trim($direction));
            if (!in_array($dir, ['ASC', 'DESC'], true)) {
                throw new \InvalidArgumentException('Invalid order direction: ' . $direction);
            }
            $this->state->orderBy[] = ['col' => $column, 'dir' => $dir];
            return $this;
        }

        public function limit(int $limit): self
        {
            $this->state->limit = max(0, $limit);
            return $this;
        }

        public function offset(int $offset): self
        {
            $this->state->offset = max(0, $offset);
            return $this;
        }

        /**
         * @param list<string>|string $columns
         * @return array<string,mixed>
         */
        public function paginate(string $table, int $page, int $perPage, array|string $columns = '*'): array
        {
            $page = max(1, $page);
            $perPage = max(1, $perPage);

            $countSql = $this->compileCountQuery($table);
            $total = (int)($this->rawQueryValue($countSql['sql'], $countSql['params']) ?? 0);

            $pages = (int)max(1, (int)ceil($total / $perPage));
            $page = min($page, $pages);
            $offset = ($page - 1) * $perPage;

            $data = $this->limit($perPage)->offset($offset)->get($table, null, $columns);

            return [
                'data' => $data,
                'pagination' => [
                    'total' => $total,
                    'page' => $page,
                    'per_page' => $perPage,
                    'pages' => $pages,
                    'has_prev' => $page > 1,
                    'has_next' => $page < $pages,
                ],
            ];
        }

        /**
         * @param list<string>|string $columns
         */
        public function get(string $table, ?int $limit = null, array|string $columns = '*'): mixed
        {
            if ($limit !== null) {
                $this->limit($limit);
            }
            $compiled = $this->compileSelectQuery($table, $columns);
            return $this->executeSelect($compiled['sql'], $compiled['params']);
        }

        /**
         * @param list<string>|string $columns
         */
        public function getOne(string $table, array|string $columns = '*'): mixed
        {
            $prevLimit = $this->state->limit;
            $this->limit(1);
            try {
                $rows = $this->get($table, null, $columns);
                if ($this->fetchMode === 'json') {
                    $decoded = json_decode((string)$rows, true);
                    if (!is_array($decoded) || $decoded === []) {
                        return null;
                    }
                    return $decoded[0] ?? null;
                }
                if (is_array($rows)) {
                    return $rows[0] ?? null;
                }
                return $rows;
            } finally {
                $this->state->limit = $prevLimit;
            }
        }

        public function getValue(string $table, string $column): mixed
        {
            $row = $this->getOne($table, [$column]);
            if ($row === null) {
                return null;
            }
            if (is_array($row)) {
                return array_values($row)[0] ?? null;
            }
            if (is_object($row)) {
                foreach ((array)$row as $v) {
                    return $v;
                }
            }
            return null;
        }

        /**
         * @param array<string,mixed> $data
         * @return string last insert id (string to match PDO)
         */
        public function insert(string $table, array $data): string
        {
            $compiled = $this->compileInsertQuery($table, $data);
            $this->executeStatement($compiled['sql'], $compiled['params']);
            $pdo = $this->connections->pdo($this->connection);
            return (string)$pdo->lastInsertId();
        }

        /**
         * @param list<array<string,mixed>> $rows
         */
        public function insertMulti(string $table, array $rows): int
        {
            $compiled = $this->compileInsertMultiQuery($table, $rows);
            return $this->executeStatement($compiled['sql'], $compiled['params']);
        }

        /**
         * @param array<string,mixed> $data
         */
        public function update(string $table, array $data): int
        {
            $compiled = $this->compileUpdateQuery($table, $data);
            return $this->executeStatement($compiled['sql'], $compiled['params']);
        }

        public function delete(string $table, ?int $limit = null): int
        {
            if ($limit !== null) {
                $this->limit($limit);
            }
            $compiled = $this->compileDeleteQuery($table);
            return $this->executeStatement($compiled['sql'], $compiled['params']);
        }

        /**
         * Raw prepared query (always uses PDO->prepare + execute).
         *
         * @param list<mixed> $params
         */
        public function rawQuery(string $sql, array $params = []): mixed
        {
            return $this->executeSelect($sql, $params);
        }

        /**
         * @param list<mixed> $params
         */
        public function rawQueryOne(string $sql, array $params = []): mixed
        {
            $prevMode = $this->fetchMode;
            $this->fetchMode = 'array';
            try {
                $rows = $this->executeSelect($sql, $params);
                if (!is_array($rows) || $rows === []) {
                    return null;
                }
                return $rows[0] ?? null;
            } finally {
                $this->fetchMode = $prevMode;
            }
        }

        /**
         * @param list<mixed> $params
         */
        public function rawQueryValue(string $sql, array $params = []): mixed
        {
            $row = $this->rawQueryOne($sql, $params);
            if ($row === null) {
                return null;
            }
            if (is_array($row)) {
                return array_values($row)[0] ?? null;
            }
            return null;
        }

        public function startTransaction(): void
        {
            $pdo = $this->connections->pdo($this->connection);
            $ident = $this->connections->ident($this->connection);
            $depth = $this->connections->txDepth($this->connection);

            if ($depth === 0) {
                $pdo->beginTransaction();
                $this->connections->setTxDepth($this->connection, 1);
                return;
            }

            if (!$ident->supportsSavepoints()) {
                throw new DbException('Nested transactions require SAVEPOINT support', [
                    'connection' => $this->connection,
                    'driver' => $ident->driver(),
                ]);
            }

            $sp = $this->savepointName($depth + 1);
            $pdo->exec('SAVEPOINT ' . $sp);
            $this->connections->setTxDepth($this->connection, $depth + 1);
        }

        public function commit(): void
        {
            $pdo = $this->connections->pdo($this->connection);
            $ident = $this->connections->ident($this->connection);
            $depth = $this->connections->txDepth($this->connection);

            if ($depth <= 0) {
                return;
            }

            if ($depth === 1) {
                $pdo->commit();
                $this->connections->setTxDepth($this->connection, 0);
                return;
            }

            if (!$ident->supportsSavepoints()) {
                throw new DbException('SAVEPOINT not supported for nested commit', [
                    'connection' => $this->connection,
                    'driver' => $ident->driver(),
                ]);
            }

            $sp = $this->savepointName($depth);
            $pdo->exec('RELEASE SAVEPOINT ' . $sp);
            $this->connections->setTxDepth($this->connection, $depth - 1);
        }

        public function rollback(): void
        {
            $pdo = $this->connections->pdo($this->connection);
            $ident = $this->connections->ident($this->connection);
            $depth = $this->connections->txDepth($this->connection);

            if ($depth <= 0) {
                return;
            }

            if ($depth === 1) {
                $pdo->rollBack();
                $this->connections->setTxDepth($this->connection, 0);
                return;
            }

            if (!$ident->supportsSavepoints()) {
                throw new DbException('SAVEPOINT not supported for nested rollback', [
                    'connection' => $this->connection,
                    'driver' => $ident->driver(),
                ]);
            }

            $sp = $this->savepointName($depth);
            $pdo->exec('ROLLBACK TO SAVEPOINT ' . $sp);
            $this->connections->setTxDepth($this->connection, $depth - 1);
        }

        /**
         * Optional hook-driven health check.
         *
         * @return bool
         */
        public function healthCheck(): bool
        {
            if ($this->hooks?->healthCheck !== null) {
                return (bool)($this->hooks->healthCheck)([
                    'connection' => $this->connection,
                ]);
            }

            try {
                $pdo = $this->connections->pdo($this->connection);
                $pdo->query('SELECT 1');
                return true;
            } catch (\Throwable) {
                return false;
            }
        }

        private function savepointName(int $depth): string
        {
            return 'sp_' . $depth;
        }

        /**
         * @param list<string>|string $columns
         * @return array{sql:string,params:list<mixed>}
         */
        private function compileSelectQuery(string $table, array|string $columns): array
        {
            $ident = $this->connections->ident($this->connection);
            $cols = is_array($columns) ? $columns : [$columns];
            if ($cols === []) {
                $cols = ['*'];
            }
            $colSql = [];
            foreach ($cols as $c) {
                $c = trim((string)$c);
                if ($c === '') {
                    continue;
                }
                $colSql[] = $ident->column($c);
            }
            if ($colSql === []) {
                $colSql = ['*'];
            }

            $sql = 'SELECT ' . implode(', ', $colSql) . ' FROM ' . $ident->table($table);
            $params = [];

            foreach ($this->state->joins as $j) {
                $sql .= ' ' . $j['type'] . ' JOIN ' . $ident->table($j['table'], $j['alias']) . ' ON ' .
                    $ident->column($j['left']) . ' ' . $j['op'] . ' ' . $ident->column($j['right']);
            }

            [$whereSql, $whereParams] = $this->compileConditions($this->state->where, 'WHERE');
            $sql .= $whereSql;
            $params = array_merge($params, $whereParams);

            if ($this->state->groupBy !== []) {
                $g = [];
                foreach ($this->state->groupBy as $c) {
                    $g[] = $ident->column($c);
                }
                $sql .= ' GROUP BY ' . implode(', ', $g);
            }

            [$havingSql, $havingParams] = $this->compileConditions($this->state->having, 'HAVING');
            $sql .= $havingSql;
            $params = array_merge($params, $havingParams);

            if ($this->state->orderBy !== []) {
                $o = [];
                foreach ($this->state->orderBy as $ord) {
                    $o[] = $ident->column($ord['col']) . ' ' . $ord['dir'];
                }
                $sql .= ' ORDER BY ' . implode(', ', $o);
            }

            if ($this->state->limit !== null) {
                $sql .= ' LIMIT ' . (int)$this->state->limit;
            }
            if ($this->state->offset !== null) {
                $sql .= ' OFFSET ' . (int)$this->state->offset;
            }

            return ['sql' => $sql, 'params' => $params];
        }

        /**
         * @return array{sql:string,params:list<mixed>}
         */
        private function compileCountQuery(string $table): array
        {
            $ident = $this->connections->ident($this->connection);
            $sql = 'SELECT COUNT(*) AS ' . $ident->identifier('cnt') . ' FROM ' . $ident->table($table);
            $params = [];

            foreach ($this->state->joins as $j) {
                $sql .= ' ' . $j['type'] . ' JOIN ' . $ident->table($j['table'], $j['alias']) . ' ON ' .
                    $ident->column($j['left']) . ' ' . $j['op'] . ' ' . $ident->column($j['right']);
            }

            [$whereSql, $whereParams] = $this->compileConditions($this->state->where, 'WHERE');
            $sql .= $whereSql;
            $params = array_merge($params, $whereParams);

            if ($this->state->groupBy !== []) {
                $g = [];
                foreach ($this->state->groupBy as $c) {
                    $g[] = $ident->column($c);
                }
                $sql .= ' GROUP BY ' . implode(', ', $g);
            }

            [$havingSql, $havingParams] = $this->compileConditions($this->state->having, 'HAVING');
            $sql .= $havingSql;
            $params = array_merge($params, $havingParams);

            return ['sql' => $sql, 'params' => $params];
        }

        /**
         * @param array<string,mixed> $data
         * @return array{sql:string,params:list<mixed>}
         */
        private function compileInsertQuery(string $table, array $data): array
        {
            $ident = $this->connections->ident($this->connection);
            if ($data === []) {
                throw new \InvalidArgumentException('insert() requires non-empty data');
            }
            $cols = [];
            $ph = [];
            $params = [];
            foreach ($data as $k => $v) {
                $cols[] = $ident->identifier((string)$k);
                $ph[] = '?';
                $params[] = $v;
            }
            $sql = 'INSERT INTO ' . $ident->table($table) . ' (' . implode(', ', $cols) . ') VALUES (' . implode(', ', $ph) . ')';
            return ['sql' => $sql, 'params' => $params];
        }

        /**
         * @param list<array<string,mixed>> $rows
         * @return array{sql:string,params:list<mixed>}
         */
        private function compileInsertMultiQuery(string $table, array $rows): array
        {
            $ident = $this->connections->ident($this->connection);
            if ($rows === []) {
                throw new \InvalidArgumentException('insertMulti() requires non-empty rows');
            }
            $first = $rows[0] ?? null;
            if (!is_array($first) || $first === []) {
                throw new \InvalidArgumentException('insertMulti() rows must be non-empty associative arrays');
            }

            $keys = array_keys($first);
            foreach ($rows as $row) {
                if (!is_array($row) || array_keys($row) !== $keys) {
                    throw new \InvalidArgumentException('insertMulti() requires rows with identical keys');
                }
            }

            $cols = [];
            foreach ($keys as $k) {
                $cols[] = $ident->identifier((string)$k);
            }

            $params = [];
            $rowsSql = [];
            $oneRowPh = '(' . implode(', ', array_fill(0, count($keys), '?')) . ')';
            foreach ($rows as $row) {
                $rowsSql[] = $oneRowPh;
                foreach ($keys as $k) {
                    $params[] = $row[$k];
                }
            }

            $sql = 'INSERT INTO ' . $ident->table($table) . ' (' . implode(', ', $cols) . ') VALUES ' . implode(', ', $rowsSql);
            return ['sql' => $sql, 'params' => $params];
        }

        /**
         * @param array<string,mixed> $data
         * @return array{sql:string,params:list<mixed>}
         */
        private function compileUpdateQuery(string $table, array $data): array
        {
            $ident = $this->connections->ident($this->connection);
            if ($data === []) {
                throw new \InvalidArgumentException('update() requires non-empty data');
            }
            if ($this->state->where === []) {
                throw new DbException('update() requires at least one where() condition');
            }

            $set = [];
            $params = [];
            foreach ($data as $k => $v) {
                $set[] = $ident->identifier((string)$k) . ' = ?';
                $params[] = $v;
            }

            $sql = 'UPDATE ' . $ident->table($table) . ' SET ' . implode(', ', $set);

            [$whereSql, $whereParams] = $this->compileConditions($this->state->where, 'WHERE');
            $sql .= $whereSql;
            $params = array_merge($params, $whereParams);

            if ($this->state->limit !== null) {
                $sql .= ' LIMIT ' . (int)$this->state->limit;
            }

            return ['sql' => $sql, 'params' => $params];
        }

        /**
         * @return array{sql:string,params:list<mixed>}
         */
        private function compileDeleteQuery(string $table): array
        {
            $ident = $this->connections->ident($this->connection);
            if ($this->state->where === []) {
                throw new DbException('delete() requires at least one where() condition');
            }

            $sql = 'DELETE FROM ' . $ident->table($table);
            [$whereSql, $whereParams] = $this->compileConditions($this->state->where, 'WHERE');
            $sql .= $whereSql;

            if ($this->state->limit !== null) {
                $sql .= ' LIMIT ' . (int)$this->state->limit;
            }

            return ['sql' => $sql, 'params' => $whereParams];
        }

        /**
         * @param list<array{bool:string,col:string,op:string,val:mixed}> $conds
         * @return array{0:string,1:list<mixed>}
         */
        private function compileConditions(array $conds, string $prefix): array
        {
            if ($conds === []) {
                return ['', []];
            }
            $ident = $this->connections->ident($this->connection);
            $sqlParts = [];
            $params = [];

            foreach ($conds as $idx => $c) {
                $bool = ($idx === 0) ? '' : (' ' . $c['bool'] . ' ');
                $col = $ident->column($c['col']);
                $op = DbSqlUtil::normalizeOperator($c['op']);

                if ($op === 'IN' || $op === 'NOT IN') {
                    if (!is_array($c['val'])) {
                        throw new DbException($op . ' requires array value');
                    }
                    $arr = array_values($c['val']);
                    if ($arr === []) {
                        $sqlParts[] = $bool . ($op === 'IN' ? '0=1' : '1=1');
                        continue;
                    }
                    $ph = implode(', ', array_fill(0, count($arr), '?'));
                    $sqlParts[] = $bool . $col . ' ' . $op . ' (' . $ph . ')';
                    foreach ($arr as $v) {
                        $params[] = $v;
                    }
                    continue;
                }

                if ($op === 'IS' || $op === 'IS NOT') {
                    if ($c['val'] !== null && !is_bool($c['val'])) {
                        throw new DbException($op . ' requires null/bool');
                    }
                    if ($c['val'] === null) {
                        $sqlParts[] = $bool . $col . ' ' . $op . ' NULL';
                    } else {
                        $sqlParts[] = $bool . $col . ' ' . $op . ' ' . ($c['val'] ? 'TRUE' : 'FALSE');
                    }
                    continue;
                }

                $sqlParts[] = $bool . $col . ' ' . $op . ' ?';
                $params[] = $c['val'];
            }

            return [' ' . $prefix . ' ' . implode('', $sqlParts), $params];
        }

        /**
         * @param list<mixed> $params
         */
        private function executeSelect(string $sql, array $params): mixed
        {
            try {
                $rows = $this->runQuery($sql, $params, true);
                if ($this->fetchMode === 'json') {
                    return json_encode($rows, $this->jsonFlags);
                }
                if ($this->fetchMode === 'object') {
                    $out = [];
                    foreach ($rows as $row) {
                        $o = new \stdClass();
                        foreach ($row as $k => $v) {
                            $o->{$k} = $v;
                        }
                        $out[] = $o;
                    }
                    return $out;
                }
                return $rows;
            } finally {
                $this->state->reset();
            }
        }

        /**
         * @param list<mixed> $params
         */
        private function executeStatement(string $sql, array $params): int
        {
            try {
                $result = $this->runQuery($sql, $params, false);
                return (int)$result;
            } finally {
                $this->state->reset();
            }
        }

        /**
         * @param list<mixed> $params
         * @return list<array<string,mixed>>|int
         */
        private function runQuery(string $sql, array $params, bool $fetchRows): array|int
        {
            $this->lastError = null;
            $this->lastQuery = null;

            $start = hrtime(true);
            $before = $this->hooks?->beforeQuery;
            if ($before !== null) {
                $before([
                    'connection' => $this->connection,
                    'sql' => $sql,
                    'params' => DbLogUtil::sanitizeParams($params),
                ]);
            }

            try {
                $stmt = $this->connections->stmt($this->connection, $sql);
                $stmt->closeCursor();
                $this->bindParams($stmt, $params);
                $stmt->execute();

                $durationMs = (hrtime(true) - $start) / 1_000_000;

                if ($fetchRows) {
                    $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
                    $this->afterSuccess($sql, $params, $durationMs, $stmt->rowCount(), $rows);
                    return is_array($rows) ? $rows : [];
                }

                $count = $stmt->rowCount();
                $this->afterSuccess($sql, $params, $durationMs, $count, null);
                return (int)$count;
            } catch (\Throwable $e) {
                $durationMs = (hrtime(true) - $start) / 1_000_000;
                $ctx = [
                    'connection' => $this->connection,
                    'sql' => $sql,
                    'params' => DbLogUtil::sanitizeParams($params),
                    'duration_ms' => $durationMs,
                    'error' => $e->getMessage(),
                    'exception' => get_debug_type($e),
                ];
                $this->lastError = $ctx;
                $this->lastQuery = [
                    'sql' => $sql,
                    'params' => DbLogUtil::sanitizeParams($params),
                    'duration_ms' => $durationMs,
                ];
                if ($this->debug && $this->debugMaxLog > 0) {
                    $this->pushLog(['ok' => false] + $ctx);
                }
                if ($this->hooks?->onError !== null) {
                    ($this->hooks->onError)($ctx);
                }

                $retry = $this->maybeRetry($sql, $params, $e, $ctx);
                if ($retry !== null) {
                    return $retry;
                }

                if ($this->errorMode === 'safe') {
                    return $fetchRows ? [] : 0;
                }
                throw new DbException('Database query failed', $ctx, $e);
            }
        }

        /**
         * @param list<mixed> $params
         */
        private function maybeRetry(string $sql, array $params, \Throwable $e, array $ctx): array|int|null
        {
            $decider = $this->hooks?->retryDecider;
            if ($decider === null) {
                return null;
            }
            $decision = $decider([
                'connection' => $this->connection,
                'sql' => $sql,
                'params' => DbLogUtil::sanitizeParams($params),
                'exception' => $e,
                'context' => $ctx,
            ]);

            if (!is_array($decision)) {
                return null;
            }

            $retries = (int)($decision['retries'] ?? 0);
            $delayMs = (int)($decision['delay_ms'] ?? 0);
            $fetchRows = (bool)($decision['fetch_rows'] ?? true);

            if ($retries <= 0) {
                return null;
            }

            $lastEx = $e;
            for ($i = 0; $i < $retries; $i++) {
                if ($delayMs > 0) {
                    usleep($delayMs * 1000);
                }
                try {
                    return $this->runQuery($sql, $params, $fetchRows);
                } catch (\Throwable $ex) {
                    $lastEx = $ex;
                }
            }

            if ($this->errorMode === 'safe') {
                return $fetchRows ? [] : 0;
            }
            throw new DbException('Database query failed after retries', $ctx, $lastEx);
        }

        /**
         * @param list<mixed> $params
         */
        private function afterSuccess(string $sql, array $params, float $durationMs, int $rowCount, ?array $rows): void
        {
            $ctx = [
                'connection' => $this->connection,
                'sql' => $sql,
                'params' => DbLogUtil::sanitizeParams($params),
                'duration_ms' => $durationMs,
                'row_count' => $rowCount,
            ];
            $this->lastQuery = $ctx;
            if ($this->debug && $this->debugMaxLog > 0) {
                $this->pushLog(['ok' => true] + $ctx);
            }
            if ($this->hooks?->afterQuery !== null) {
                ($this->hooks->afterQuery)($ctx + ['rows' => $rows]);
            }
        }

        private function pushLog(array $entry): void
        {
            $this->queryLog[] = $entry;
            if (count($this->queryLog) > $this->debugMaxLog) {
                array_shift($this->queryLog);
            }
        }

        /**
         * @param list<mixed> $params
         */
        private function bindParams(\PDOStatement $stmt, array $params): void
        {
            $i = 1;
            foreach ($params as $v) {
                if ($v === null) {
                    $stmt->bindValue($i, null, \PDO::PARAM_NULL);
                } elseif (is_int($v)) {
                    $stmt->bindValue($i, $v, \PDO::PARAM_INT);
                } elseif (is_bool($v)) {
                    $stmt->bindValue($i, $v, \PDO::PARAM_BOOL);
                } else {
                    $stmt->bindValue($i, (string)$v, \PDO::PARAM_STR);
                }
                $i++;
            }
        }
    }
}

if (!class_exists('DbSqlUtil', false)) {
    final class DbSqlUtil
    {
        /**
         * @return array{0:string,1:string,2:string}
         */
        public static function parseSimpleBinaryExpr(string $expr): array
        {
            $expr = trim($expr);
            $m = [];
            if (!preg_match('/^([a-zA-Z_][a-zA-Z0-9_\\.\\*]*)\\s*(=|<>|!=|<=|>=|<|>)\\s*([a-zA-Z_][a-zA-Z0-9_\\.\\*]*)$/', $expr, $m)) {
                throw new DbException('Unsafe/unsupported join expression');
            }
            return [$m[1], $m[2], $m[3]];
        }

        public static function normalizeOperator(string $op): string
        {
            $op = strtoupper(trim($op));
            $allowed = [
                '=', '!=', '<>', '<', '>', '<=', '>=',
                'LIKE',
                'IN', 'NOT IN',
                'IS', 'IS NOT',
            ];
            if (!in_array($op, $allowed, true)) {
                throw new DbException('Unsupported operator: ' . $op);
            }
            return $op;
        }
    }
}

if (!function_exists('db')) {
    /**
     * Convenience factory.
     *
     * @param array<string,DbConfig|array<string,mixed>> $connections
     */
    function db(array $connections, string $default = 'default', ?DbHooks $hooks = null): PdoDb
    {
        return PdoDb::create($connections, $default, $hooks);
    }
}

