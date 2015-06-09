<?hh // strict
namespace Decouple\DBAL\Query;
class PreparedQuery {
  public function __construct(public string $query, public Map<string,mixed> $values) {}
}
