<?hh // strict
namespace Decouple\DBAL\Schema;
use Decouple\DBAL\Driver\AbstractMySQLDriver;
use Decouple\DBAL\Table\MySQLTable;
use Decouple\Common\Contract\DB\Schema;
class MySQLSchema implements Schema {
  public function __construct(protected string $name, protected AbstractMySQLDriver $driver) {

  }
  public function exists() : bool {
    $statement = $this->driver->prepare('SELECT COUNT(1) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?');
    $statement->execute(Vector{ $this->name });
    $count = $statement->fetchColumn();
    return $count ? true : false;
  }
  public function create() : bool {
    $statement = $this->driver->prepare(sprintf('CREATE DATABASE %s', $this->name));
    $res = $statement->execute();
    $this->connector()->connection()->query(sprintf('USE %s', $this->name));
    return $res ? true : false;
  }
  public function table(string $name) : MySQLTable {
    return new MySQLTable($name, $this);
  }
  public function driver() : AbstractMySQLDriver {
    return $this->driver;
  }
  public function execute(string $query) : bool {
    return $this->driver->execute($query);
  }
}
