<?hh // strict
namespace Decouple\DBAL\Table;
use Decouple\Common\Contract\DB\Schema;
use Decouple\Common\Contract\DB\Table;
use Decouple\Common\Contract\DB\TableStructure;
use Decouple\Common\Contract\DB\Query;
use Decouple\Common\Contract\DB\Queryable;
use Decouple\Common\Contract\DB\ExecutableTableStructure;
use Decouple\DBAL\Table\Alter\MySQLTableAlter;
use Decouple\DBAL\Table\Create\MySQLTableCreate;
use Decouple\DBAL\Query\MySQLQuery;
use Decouple\DBAL\Query\AbstractMySQLQuery;
use Decouple\DBAL\Query\AwaitableMySQLQuery;
class MySQLTable implements Table {
  public function __construct(
    protected string $name,
    protected Schema $schema,
  ) {}
  public function create(
    (function(ExecutableTableStructure): void) $callback,
  ): bool {
    $create = new MySQLTableCreate($this);
    $callback($create);
    return $create->execute();
  }
  public function getCreate(): MySQLTableCreate {
    return new MySQLTableCreate($this);
  }
  public function alter(
    (function(ExecutableTableStructure): void) $callback,
  ): bool {
    $alter = new MySQLTableAlter($this);
    $callback($alter);
    return $alter->execute();
  }
  public function getAlter(): MySQLTableAlter {
    return new MySQLTableAlter($this);
  }
  public function drop(): bool {
    return
      $this->schema
        ->driver()
        ->execute(sprintf('DROP TABLE IF EXISTS %s', $this->name));
  }
  public function delete(bool $soft=false): MySQLQuery {
    $query = new MySQLQuery($this);
    return $query->delete($soft);
  }
  public function exists(): bool {
    $statement = $this->schema->driver()->prepare('SHOW TABLES LIKE ?');
    $statement->execute(Vector {$this->name});
    if ($statement->rowCount()) {
      return true;
    }
    return false;
  }
  public function insert(Map<string, mixed> $data): int {
    $query = new MySQLQuery($this);
    return $query->insert($data);
  }
  public function update(Map<string, mixed> $data): MySQLQuery {
    $query = new MySQLQuery($this);
    return $query->update($data);
  }
  public function select(?Vector<string> $fields = null): MySQLQuery {
    return (new MySQLQuery($this))->select($fields);
  }
  public function selectColumn(string $field): MySQLQuery {
    return (new MySQLQuery($this))->selectColumn($field);
  }
  public function getName(): string {
    return $this->name;
  }
  public function schema(): Schema {
    return $this->schema;
  }
  public function fetchAll(): Vector<Map<string, mixed>> {
    return $this->select()->fetchAll();
  }
  public function first(): ?Map<string, mixed> {
    return $this->select()->first();
  }
  public function limit(int $min = 0, int $max = 25): Query {
    return $this->select()->limit($min, $max);
  }
  public function orderBy(
    string $field,
    string $direction = 'DESC',
  ): MySQLQuery {
    return $this->select()->orderBy($field, $direction);
  }
  public function where(
    string $field,
    string $comp,
    mixed $value,
  ): MySQLQuery {
    return $this->select()->where($field, $comp, $value);
  }
  public function whereAll(
    KeyedTraversable<string, string> $array,
  ): MySQLQuery {
    return $this->select()->whereAll($array);
  }
  public function awaitable() : AwaitableMySQLQuery {
    return new AwaitableMySQLQuery($this);
  }
}
