<?hh // strict
namespace Decouple\DBAL\Table\Structure;
class TableStructure extends AbstractTableStructure {
  public function __construct(string $name) {
    $this->columns = Map {};
    parent::__construct($name);
  }
  public function execute() : bool {
    return false;
  }
}
