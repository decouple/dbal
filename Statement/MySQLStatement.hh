<?hh // strict
use Decouple\Common\Contract\DB\Statement;
interface MySQLStatement extends Statement {
  public function fetchColumn(int $column=0) : mixed;
}
