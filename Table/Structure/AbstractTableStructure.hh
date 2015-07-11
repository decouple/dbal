<?hh // strict
namespace Decouple\DBAL\Table\Structure;
use Decouple\Common\Contract\DB\TableStructure as TableStructureContract;
use Decouple\Common\Contract\DB\TableColumn as TableColumnContract;
abstract class AbstractTableStructure implements TableStructureContract {
  protected Map<string,TableColumnContract> $columns;
  public function __construct(protected string $name) {
	$this->columns = Map {};
  }
  public function getName() : string {
    return $this->name;
  }
  public function integer(string $name, int $length=10) : TableColumnContract {
    $column = new TableColumn($name, 'integer', Map {'length' => $length});
    $this->columns[$name] = $column;
    return $column;
  }
  public function increments(string $name='id', int $length=14) : TableColumnContract {
    $column = new TableColumn($name, 'integer', Map {'length' => $length, 'auto_increment' => true});
    $this->columns[$name] = $column;
    return $column;
  }
  public function string(string $name, int $length=55) : TableColumnContract {
    $column = new TableColumn($name, 'string', Map {'length' => $length});
    $this->columns[$name] = $column;
    return $column;
  }
  public function text(string $name) : TableColumnContract {
    $column = new TableColumn($name, 'text');
    $this->columns[$name] = $column;
    return $column;
  }
  public function enum(string $name, Traversable<string> $values) : TableColumnContract {
    $column = new TableColumn($name, 'enum');
    $column->setAttribute('values', $values);
    $this->columns[$name] = $column;
    return $column;
  }
  public function decimal(string $name, int $numbers=7, int $decimals=2) : TableColumnContract {
    $column = new TableColumn($name, 'decimal');
    $column->setAttribute('numbers', $numbers);
    $column->setAttribute('decimals', $decimals);
    $this->columns[$name] = $column;
    return $column;
  }
  public function foreign(string $name) : ForeignTableColumn {
    $column = new ForeignTableColumn($name);
    $this->columns[$name] = $column;
    return $column;
  }
  public function timestamp(string $name, bool $value=true) : void {
    $created_at = new TableColumn($name, 'timestamp');
    $this->columns[$name] = $created_at;
  }
  public function softDeletes(bool $value=true) : void {
    $deleted_at = new TableColumn('deleted_at', 'timestamp');
    $this->columns['deleted_at'] = $deleted_at;
  }
  public function timestamps(bool $value=true) : void {
    $created_at = new TableColumn('created_at', 'timestamp');
    $this->columns['created_at'] = $created_at;
  $updated_at = new TableColumn('updated_at', 'timestamp');
    $this->columns['updated_at'] = $updated_at;
  }
  public function addColumn(TableColumnContract $column) : void {
      $this->columns->set($column->getName(), $column);
  }
  public function dropColumn(string $column) : void {
      $this->columns->remove($column);
  }
  public function getColumn(string $name) : ?TableColumnContract {
    return $this->columns->contains($name) ? $this->columns->at($name) : null;
  }
  public function getColumns() : Map<string,TableColumnContract> {
    return $this->columns;
  }
  public function toString(): string {
    return serialize($this->columns);
  }
  public function fromString(string $string): void {
    $this->columns = new Map(unserialize($string));
  }
}
