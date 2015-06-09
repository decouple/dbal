<?hh // strict
namespace Decouple\DBAL\Contract;
use Decouple\Common\Contract\DB\ExecutableTableStructure;
interface TableCreateInterface extends ExecutableTableStructure {
  public function execute(bool $alter=false) : bool;
}
