<?hh // strict
namespace Decouple\DBAL\Table\Alter;
use Decouple\DBAL\Table\MySQLTable;
use Decouple\DBAL\Table\Structure\MySQLTableStructure;
use Decouple\DBAL\Contract\TableAlterInterface;
use Decouple\Common\Contract\DB\TableColumn;
use Decouple\Common\Contract\AbstractAttributeContainer;
class MySQLTableAlter extends MySQLTableStructure
  implements TableAlterInterface {
  protected Vector<string> $dropped;
  protected Vector<TableColumn> $changed;
  public function __construct(protected MySQLTable $table) {
    parent::__construct($table->getName());
    $this->dropped = Vector {};
    $this->changed = Vector {};
  }

  public function dropColumn(string $column): void {
    $this->dropped->add($column);
  }

  public function alterColumn(TableColumn $column): void {
    $this->changed->add($column);
  }

  public function execute(): bool {
    $sql = "ALTER TABLE ".$this->table->getName()."\n";
    if ($this->columns->count()) {
      foreach ($this->columns as $column) {
        $sql .= "ADD ".$this->columnDefinition($column)."\n";
      }
    }
    if ($this->dropped->count()) {
      foreach ($this->dropped as $column) {
        $sql .= "DROP COLUMN ".$column.",\n";
      }
    }
    if ($this->changed->count()) {
      foreach ($this->changed as $column) {
        $sql .=
          "MODIFY COLUMN ".
          $this->columnDefinition($column, true).
          "\n";
      }
    }
    $sql = substr($sql, 0, -2).';';
    return $this->table->schema()->execute($sql);
  }
}
