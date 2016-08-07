<?hh // strict
namespace Decouple\DBAL\Query;
use Decouple\DBAL\Table\MySQLTable;
use Decouple\Common\Contract\DB\Statement;
use Decouple\Common\Contract\DB\Queryable;
use Decouple\Common\Contract\DB\Query;
use Exception;
abstract class AbstractMySQLQuery {
  protected string $action;
  protected Map<string, mixed> $data;
  protected Vector<string> $selectFields;
  protected Vector<Vector<mixed>> $whereData;
  protected Map<string, string> $orderData;
  protected MySQLTable $table;
  protected int $limitMin = 0;
  protected int $limitMax = 0;
  public string $query_raw;

  public function build(): PreparedQuery {
    $query = '';
    if ($this->action == 'select') {
      $query =
        'SELECT '.
        implode(',', $this->selectFields).
        ' FROM '.
        $this->table->getName();
    } else if ($this->action == 'update') {
      $query = 'UPDATE '.$this->table->getName().' SET ';
      foreach ($this->data as $var => $val) {
        if ($val instanceof Raw) {
          $query .= $var.' = '.(string) $val->getValue();
        } elseif ($var !== 'modified') {
          $query .= $var.' = :'.$var.', ';
        }
      }
      $query .= 'modified = FROM_UNIXTIME(:modified)';
    } else if ($this->action == 'insert') {
      $columns = [];
      $values = [];
      foreach ($this->data as $var => $val) {
        $columns[] = '`'.$var.'`';
        if ($val instanceof Raw) {
          $values[] = $val->getValue();
        } else {
          $values[] = ':'.$var;
        }
      }
      $columns = implode(', ', $columns);
      $values = implode(', ', $values);
      $query =
        'INSERT INTO '.
        $this->table->getName().
        ' ('.
        $columns.
        ') VALUES ('.
        $values.
        ')';
    } else if ($this->action == 'delete') {
      $query = 'DELETE FROM '.$this->table->getName();
    }

    $whereFields = Map {};
    if (count($this->whereData) > 0) {
      $query .= ' WHERE ';
      foreach ($this->whereData as $vec) {
        $field = (string) $vec[0];
        $operator = (string) $vec[1];
        $value = $vec[2];
        $standIn = ':'.$field;
        if (is_null($value)) {
          $operator = ' IS ';
          $val = $standIn = 'NULL';
        } elseif (is_array($value)) {
          $val = json_encode($value);
        } elseif ($value instanceof Raw) {
          $val = $value->getValue();
          $standIn = (string) $val;
        } else {
          $val = $value;
        }
        $whereFields[(string) $vec[0]] = $val;

        $query .= sprintf('`%s`%s%s AND ', $field, $operator, $standIn);
      }
      $query = substr($query, 0, -4);
    }

    if($this->action == 'select') {
      if ($this->orderData->count()) {
        $query .= ' ORDER BY ';
        $orderParts = Vector {};
        foreach ($this->orderData as $field => $dir) {
          $orderParts->add(sprintf('%s %s', $field, $dir));
        }
        $query .= implode(', ', $orderParts);
      }
      if($this->limitMax > 0) {
        $query .= sprintf(' LIMIT %s, %s', $this->limitMin, $this->limitMax);
      }
    }

    $fields = Map {};
    $fields->setAll($whereFields);
    $fields->setAll($this->data);
    return new PreparedQuery($query, $fields);
  }

  public function raw(mixed $value): Raw {
    return new Raw($value);
  }

  public function reset(): void {
    $this->selectFields = Vector {};
    $this->data = Map {};
    $this->whereData = Vector {};
    $this->action = 'select';
  }

  public function debugQuery(
    PreparedQuery $query,
    Map<string, mixed> $values,
  ): void {
    $sql = $query->query;
    foreach ($values as $str => $val) {
      if (is_string($val))
        $val = "'$val'";
      $sql = str_replace(':'.$str, $val, $sql);
    }
    echo $sql."\n";
  }
}
