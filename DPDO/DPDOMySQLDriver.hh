<?hh // strict
namespace Decouple\DBAL\DPDO;
use Decouple\DBAL\Driver\AbstractMySQLDriver;
use Decouple\DBAL\Schema\MySQLSchema;
use Decouple\Registry\Registry;
use Decouple\DBAL\Statement;
use Decouple\DBAL\Query;
use Decouple\DBAL\DPDO\DPDOConnector;
use Decouple\DBAL\DPDO\DPDOStatement;
use Decouple\DBAL\Driver\DriverException;
class DPDOMySQLDriver extends AbstractMySQLDriver {
  protected DPDOConnector $pdo;
  protected Map<string,mixed> $options;
  public function __construct() {
    $this->errors = Vector {};
    $this->options = Map {};
    $this->pdo = new DPDOConnector();
  }
  public function connect(Map<string,string> $params, string $username, string $password, Map<string,mixed> $options=Map {}) : bool {
    return $this->pdo->connect($params, $username, $password, $options);
  }
  public function getName() : string {
    return 'pdo_mysql';
  }
  public function prepare(string $query) : DPDOStatement {
    $statement = $this->pdo->prepare($query);
    if(is_null($statement)) {
      throw new DriverException("Unhandled Exception");
    }
    return $statement;
  }
  public function connector() : DPDOConnector {
    return $this->pdo;
  }
  public function schema(string $name) : MySQLSchema {
    return parent::schema($name);
  }
  public function execute(string $query) : bool {
    $res = $this->connector()->connection()->query($query);
    return $res ? true : false;
  }
}
