# 🚀 PdoDb — Future-Proof PDO Database Core

Production-grade, single-file PDO execution core and lightweight query builder for PHP.

Provides a secure, ergonomic, and efficient way to interact with databases using prepared statements, strict validation, and zero-dependency architecture.

Based on long-term production backend design principles. :contentReference[oaicite:1]{index=1}

---

# 🌍 Repository Setup

Default recommended GitHub setup:

- Visibility: Public
- README: Enabled
- License: Optional (MIT recommended for open source)
- .gitignore: Optional (can be added later)

---

# 🎯 Project Philosophy

This project is designed using **Principal / Lead Architect backend standards**:

✅ Security First  
✅ Prepared Statements Only  
✅ Deterministic Query Execution  
✅ Zero Framework Lock-In  
✅ Zero Runtime Dependencies  
✅ Long-Term Maintainability (10+ years mindset)  
✅ Production Infrastructure Friendly  

---

# 📦 Core Features

## ✅ Single File Architecture
Drop into any project instantly.

No Composer required.  
No dependency hell.  

---

## ✅ PDO Based Execution Core
Supports:

- MySQL / MariaDB
- PostgreSQL
- SQLite (driver dependent)

---

## ✅ Lightweight Query Builder
Fluent builder similar to MysqliDb ergonomics. :contentReference[oaicite:2]{index=2}

---

## ✅ Multiple Connections
Supports named connections and runtime switching.

---

## ✅ Nested Transactions
Supports SAVEPOINT-based nested transaction logic. :contentReference[oaicite:3]{index=3}

---

## ✅ Hooks System
Built-in extension points:

- Query Logging
- Metrics
- Auditing
- Retry logic

---

## ✅ Pagination Support
Built-in pagination helper.

---

# 🧱 Installation

Just include file:

```php
require_once 'PdoDb.php';
```

---

# ⚡ Initialization

```php
$db = PdoDb::create([
    'default' => [
        'dsn' => 'mysql:host=localhost;dbname=test_db;charset=utf8mb4',
        'username' => 'root',
        'password' => 'secret'
    ]
]);
```

---

# 📊 Basic Usage

---

## SELECT — Get Multiple Rows

```php
$users = $db->get('users');
```

---

## SELECT — With Conditions

```php
$users = $db
    ->where('active', 1)
    ->orderBy('created_at', 'DESC')
    ->limit(10)
    ->get('users');
```

---

## SELECT — Get One Row

```php
$user = $db->where('id', 42)->getOne('users');
```

---

## SELECT — Get Single Value

```php
$count = $db->getValue('users', 'COUNT(*)');
```

---

# ✏️ Insert Data

---

## Insert Single Row

```php
$id = $db->insert('users', [
    'username' => 'john',
    'email' => 'john@example.com'
]);
```

---

## Insert Multiple Rows

```php
$db->insertMulti('users', [
    ['username' => 'user1'],
    ['username' => 'user2']
]);
```

---

# 🔄 Update Data

```php
$db->where('id', 42)->update('users', [
    'active' => 0
]);
```

---

# ❌ Delete Data

```php
$db->where('last_login', '2023-01-01', '<')
   ->delete('users');
```

---

# 🔗 Query Builder Methods

---

## WHERE

```php
$db->where('id', 1);
$db->orWhere('status', 'active');
```

---

## JOIN

```php
$db->join('profiles', 'users.id = profiles.user_id', 'INNER');
```

---

## GROUP BY

```php
$db->groupBy('role');
```

---

## ORDER BY

```php
$db->orderBy('created_at', 'DESC');
```

---

## LIMIT / OFFSET

```php
$db->limit(10)->offset(20);
```

---

# 🔥 Raw Queries (Still Safe)

---

## Multiple Rows

```php
$db->rawQuery("SELECT * FROM users WHERE id > ?", [100]);
```

---

## One Row

```php
$db->rawQueryOne("SELECT * FROM users WHERE id = ?", [1]);
```

---

## Single Value

```php
$db->rawQueryValue("SELECT COUNT(*) FROM users");
```

---

# 📄 Pagination

```php
$result = $db->paginate('users', 1, 20);
```

Returns:

```
[
  data => [...],
  pagination => ...
]
```

---

# 🔐 Transactions

---

## Basic Transaction

```php
$db->startTransaction();

try {
    $db->insert('logs', ['msg' => 'start']);
    $db->commit();
} catch (Exception $e) {
    $db->rollback();
}
```

---

## Nested Transaction

```php
$db->startTransaction();
$db->startTransaction(); // Savepoint
$db->commit();
$db->commit();
```

---

# 🌍 Multiple Connections

```php
$analytics = $db->withConnection('analytics');
```

---

# 🧪 Debug Mode

```php
$db->debug(true);
print_r($db->queryLog());
```

---

# 🔌 Hooks Example

```php
$hooks->afterQuery = function($ctx) {
   if ($ctx['duration_ms'] > 500) {
       error_log("Slow Query");
   }
};
```

---

# 🔒 Security Model

✔ Prepared Statements Only  
✔ Safe Parameter Binding  
✔ Identifier Validation  
✔ No SQL Injection Risk  

---

# ⚡ Performance Design

Optimized for:

- OPcache
- Low memory allocation
- Statement reuse
- Future connection pooling

---

# 🚫 What This Is NOT

❌ Not ORM  
❌ Not ActiveRecord  
❌ Not Framework  
❌ Not Migration Tool  

---

# 🏁 Production Use Cases

✔ SaaS Backends  
✔ REST APIs  
✔ Microservices  
✔ High Traffic Systems  
✔ Enterprise PHP Systems  

---

# 📜 License

MIT
