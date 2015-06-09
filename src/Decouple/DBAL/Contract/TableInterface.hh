<?hh // strict
namespace Decouple\DBAL\Table;
use Decouple\Common\Contract\DB\Table;
use Decouple\Common\Contract\DB\ExecutableTableStructure;
interface TableInterface extends Table {
  public function exists() : bool;
  public function create((function(ExecutableTableStructure):void) $callback) : bool;
  public function alter((function(ExecutableTableStructure):void) $callback) : bool;
  public function getCreate() : ExecutableTableStructure;
  public function getAlter() : ExecutableTableStructure;
  public function drop() : bool;
}
