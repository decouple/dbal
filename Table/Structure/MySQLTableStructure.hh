<?hh // strict
namespace Decouple\DBAL\Table\Structure;
use Decouple\Common\Contract\DB\TableColumn as TableColumnContract;
use Decouple\Common\Contract\DB\TableStructure as TableStructureContract;
class MySQLTableStructure extends AbstractTableStructure
  implements TableStructureContract {
  protected Map<string, TableColumnContract> $columns;
  public function __construct(string $name) {
    $this->columns = Map {};
    parent::__construct($name);
  }
  public function columnDefinition(
    TableColumnContract $column,
    bool $alter = false,
  ): string {
    $ctype = "VARCHAR(255)";
    $extras = [];

    if ($column->getAttribute('default')) {
      $value = $column->getAttribute('default');
      if (is_string($value) && $value != 'NULL' &&
          (substr($value, 0, 1) !== '`' || substr($value, -1, 1) !== '`')) {
        $value = "'".(string) addslashes($value)."'";
      } else {
        $value = (string) str_replace('`', '', $value);
      }
      $line = sprintf('DEFAULT %s', $value);
      $extras[] = $line;
    }

    $type = $column->getType();
    if ($type == "integer" || $type == "increments") {
      $ctype = sprintf("INT(%s)", $column->getAttribute('length') ?: 14);
      if ($column->getAttribute('auto_increment')) {
        $ctype .= " UNSIGNED";
        $extras[] = "AUTO_INCREMENT";
      }
    } else if ($type == "varchar" || $type == "string") {
      $ctype =
        sprintf("VARCHAR(%s)", $column->getAttribute('length') ?: 55);
    } else if ($type == "text") {
      $ctype = "TEXT";
    } else if ($type == "enum") {
      $ctype = sprintf(
        "ENUM('%s')",
        implode("','", $column->getAttribute('values')),
      );
    } else if ($type == "timestamp") {
      $ctype = "TIMESTAMP";
      if(!$column->hasAttribute('default')) {
        $extras[] = 'default NOW()';
      }
    } else if ($type == 'foreign') {
      $ctype = 'INT(14) UNSIGNED';
    }

    // Handle signed vs unsigned
    if ($column->getAttribute('unsigned')) {
      $ctype .= ' UNSIGNED';
    }
    // Handle nullability
    $null = '';
    if ($column->getAttribute('nullable')) {
      $null = 'NULL';
    } else {
      if ($type != "enum") {
        $null = 'NOT NULL';
      }
    }
    // Unique
    if ($column->getAttribute('unique')) {
      $extras[] = 'UNIQUE';
    }

    $definition = sprintf(
      "`%s` %s %s %s",
      $column->getName(),
      $ctype,
      $null,
      implode(' ', $extras),
    );
    return $definition;
  }

  public function columnAfter(TableColumnContract $column): ?string {
    $result = null;
    $type = $column->getType();
    if ($type == "integer") {
      if ($column->getAttribute('auto_increment')) {
        $result .= sprintf("PRIMARY KEY (%s)", $column->getName());
      }
    } else if ($column instanceof ForeignTableColumn) {
      $result .= sprintf(
        "FOREIGN KEY (%s)\nREFERENCES %s(%s)",
        $column->getName(),
        $column->getAttribute('on'),
        $column->getAttribute('references'),
      );
      if ($column->hasAttribute('ondelete')) {
        $ondelete = (string) $column->getAttribute('ondelete');
        $result .= sprintf("\nON DELETE %s", strtoupper($ondelete));
      }
    }
    return $result;
  }
}
