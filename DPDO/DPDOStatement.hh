<?hh // strict
namespace Decouple\DBAL\DPDO;
use Decouple\Common\Contract\DB\Statement;
use PDOStatement;
class DPDOStatement extends PDOStatement implements Statement {
  public function fetchColumn(int $columnIndex = 0) : mixed
  {
    return parent::fetchColumn($columnIndex);
  }
  public function execute(mixed $params=null) : mixed {
    return parent::execute($params);
  }
  public function bindParam(int $paramno, mixed $param, ?string $type = null, int $max_value_len = 0, mixed $driver_params = null) : void {
    parent::bindParam($paramno, $param, $type, $max_value_len, $driver_params);
  }
  public function errorCode() : int {
    return (int) parent::errorCode();
  }
  public function errorInfo() : Traversable<mixed> {
    return parent::errorInfo();
  }
  public function rowCount() : int {
    return (int) parent::rowCount();
  }
}
