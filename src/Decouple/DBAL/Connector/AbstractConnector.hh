<?hh // strict
namespace Decouple\DBAL\Connector;
use Decouple\Common\Contract\DB\Connector;
abstract class AbstractConnector implements Connector {
  protected bool $throwsExceptions=false;
  protected bool $isConnected=false;
  protected Vector<ConnectorException> $errors;
  abstract public function connect(Map<string,string> $params, string $username, string $password, Map<string,mixed> $options=Map {}) : bool;
  public function isConnected() : bool {
    return $this->isConnected;
  }
  public function hasErrors() : bool {
    return $this->errors->count() ? true : false;
  }
  public function throwExceptions(bool $throws) : void {
    $this->throwsExceptions = $throws;
  }
  public function throwsExceptions() : bool {
    return $this->throwsExceptions;
  }
}
