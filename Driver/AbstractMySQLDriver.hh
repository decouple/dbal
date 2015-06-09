<?hh // strict
namespace Decouple\DBAL\Driver;
use Decouple\DBAL\Schema\MySQLSchema;
use Decouple\Common\Contract\DB\Driver;
use Decouple\Common\Contract\DB\Statement;
use Exception;
abstract class AbstractMySQLDriver implements Driver {
  protected bool $throwsExceptions=false;
  protected bool $isConnected=false;
  protected Vector<Exception> $errors;
  abstract public function prepare(string $query): Statement;
  public function isConnected() : bool {
    return $this->isConnected;
  }
  public function hasErrors() : bool {
    return $this->errors->count() ? true : false;
  }
  public function getErrors() : Vector<Exception> {
    return $this->errors;
  }
  public function throwExceptions(bool $throws) : void {
    $this->throwsExceptions = $throws;
  }
  public function throwsExceptions() : bool {
    return $this->throwsExceptions;
  }
  public function schema(string $name) : MySQLSchema {
    return new MySQLSchema($name, $this);
  }
}
