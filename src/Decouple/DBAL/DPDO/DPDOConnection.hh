<?hh // strict
namespace Decouple\DBAL\DPDO;
use Decouple\DBAL\Driver\DriverException;
class DPDOConnection extends \PDO {
  public function __construct(string $dsn, ?string $user=null, ?string $password = null, Map<string,mixed> $options=Map {})
  {
    try {
      parent::__construct($dsn, $user, $password, $options->toArray());
      $this->setAttribute(\PDO::ATTR_STATEMENT_CLASS, array('Decouple\DBAL\DPDO\DPDOStatement'));
      $this->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    } catch(\PDOException $e) {
      throw new DriverException($e->getMessage());
    }
  }

  public function lastInsertId(?string $name=NULL) : int {
    return intval(parent::lastInsertId($name));
  }
}
