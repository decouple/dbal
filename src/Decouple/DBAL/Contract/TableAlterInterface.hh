<?hh // strict
namespace Decouple\DBAL\Contract;
use Decouple\Common\Contract\DB\TableColumn;
use Decouple\Common\Contract\DB\ExecutableTableStructure;
interface TableAlterInterface extends ExecutableTableStructure {
  public function dropColumn(string $column) : void;
  public function alterColumn(TableColumn $column) : void;
  public function execute() : bool;
  public function fromString(string $string) : void;
  public function toString() : string;
  public function getName() : string;
}
