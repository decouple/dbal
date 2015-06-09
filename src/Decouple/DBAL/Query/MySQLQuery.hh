<?hh // strict
namespace Decouple\DBAL\Query;
use Decouple\DBAL\Table\MySQLTable;
use Decouple\Common\Contract\DB\Statement;
use Decouple\Common\Contract\DB\Query;
use Exception;
class MySQLQuery extends AbstractMySQLQuery implements Query {
  protected string $action;
  protected Map<string, mixed> $data;
  protected Vector<string> $selectFields;
  protected Vector<Vector<mixed>> $whereData;
  protected Map<string, string> $orderData;
  protected int $limitMin = 0;
  protected int $limitMax = 0;
  public string $query_raw;

  public function __construct(protected MySQLTable $table) {
    $this->action = 'select';
    $this->data = Map {};
    $this->selectFields = Vector {};
    $this->whereData = Vector {};
    $this->orderData = Map {};
    $this->query_raw = '';
  }

  public function limit(int $min=0, int $max=25) : MySQLQuery {
    $this->limitMin = $min;
    $this->limitMax = $max;
    return $this;
  }

  public function select(?Vector<string> $fields = null): MySQLQuery {
    $this->action = 'select';
    if (is_null($fields)) {
      $fields = Vector {"*"};
    }
    if (!is_null($fields)) {
      $this->selectFields->addAll($fields);
    }
    return $this;
  }

  public function selectColumn(string $field): MySQLQuery {
    $this->action = 'select';
    $this->selectFields->add($field);
    return $this;
  }

  public function update(Map<string, mixed> $data): MySQLQuery {
    $this->action = 'update';
    $data['modified'] = time();
    $this->data->setAll($data);
    return $this;
  }

  public function delete(bool $soft = false): MySQLQuery {
    if (!$soft) {
      $this->action = 'delete';
      return $this;
    } else {
      $this->action = 'update';
      $data = Map {"deleted" => time()};
      $this->data->setAll($data);
      return $this;
    }
  }

  public function where(
    string $field,
    string $comp,
    mixed $value,
  ): MySQLQuery {
    $this->whereData->add(Vector {$field, $comp, $value});
    return $this;
  }

  public function whereAll(
    KeyedTraversable<string, string> $array,
  ): MySQLQuery {
    foreach ($array as $key => $where) {
      if (count($where) == 3) {
        $this->where($where[0], $where[1], $where[2]);
      } else {
        if (is_string($key) && (is_string($where) || is_numeric($where))) {
          $this->where($key, '=', $where);
        }
      }
    }
    return $this;
  }

  public function orderBy(
    string $field,
    string $direction = 'DESC',
  ): MySQLQuery {
    $this->orderData->set($field, $direction);
    return $this;
  }

  public function insert(Map<string, mixed> $data): int {
    $this->action = 'insert';
    $this->data->setAll($data);
    $this->execute();
    $id = $this->table->schema()->driver()->connector()->lastInsertId();
    return $id;
  }

  public function fetchAll(): Vector<Map<string, mixed>> {
    $fetched = $this->execute()->fetchAll();
    $result = Vector {};
    foreach ($fetched as $row) {
      $result[] = $row;
    }
    return $result;
  }

  public function first(): ?Map<string, mixed> {
    $this->limitMin = 0;
    $this->limitMax = 1;
    $fetched = $this->execute()->fetchAll();
    if(!count($fetched)) {
      return null;
    }
    return new Map($fetched[0]);
  }

  public function fetchColumn(int $column = 0): mixed {
    return $this->execute()->fetchColumn($column);
  }

  public function execute(): Statement {
    $build = $this->build();
    $statement = $this->table->schema()->driver()->prepare($build->query);
    $values = Map {};
    foreach ($build->values as $index => $value) {
      if ($value === '') {
        $values->set($index, null);
      } else if (!$value instanceof Raw) {
        $values->set($index, $value);
      }
    }
    $statement->execute($values);
    return $statement;
  }
}
