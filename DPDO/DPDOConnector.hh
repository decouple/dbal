<?hh // strict
namespace Decouple\DBAL\DPDO;
use Decouple\DBAL\Connector\AbstractConnector;
use Decouple\DBAL\DPDO\DPDOStatement;
use Decouple\DBAL\Driver\DriverException;
use Decouple\DBAL\DPDO\DPDOConnection;
class DPDOConnector extends AbstractConnector {
  private ?DPDOConnection $connection;
  public function __construct() {
    $this->errors = Vector{};
  }
  public function connect(Map<string,string> $params, string $username, string $password, Map<string,mixed> $options=Map {}) : bool {
    $this->connection = new DPDOConnection(static::dsn($params), $username, $password);
    return $this->connection ? true : false;
  }
  static function dsn(Map<string,string> $params) : string {
    /* Borrowed heavily from Doctrine's DBAL/Driver/PDOMySql/Driver */
    // Assume MySQL
    $type = 'mysql';
    if($params->contains('type')) {
      $type = $params->at('type');
      $params->remove('type');
    }
    $dsn = sprintf('%s:', $type);
    foreach($params as $param => $value) {
      $dsn .= sprintf('%s=%s;', $param, $value);
    }
    return $dsn;
  }
  public function connection() : DPDOConnection {
    if(is_null($this->connection)) {
      throw new DriverException("Driver not connected");
    }
    return $this->connection;
  }
  public function isConnected() : bool {
    if(is_null($this->connection)) {
      return false;
    }
    return true;
  }
  public function prepare(string $query) : DPDOStatement {
    return $this->connection()->prepare($query);
  }
  public function lastInsertId() : int {
    return $this->connection()->lastInsertId();
  }
}
