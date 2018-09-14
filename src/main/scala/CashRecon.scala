case class MarketValue(value: Int) {
  def isNotZero: Boolean = this.value != 0

  def isGreaterThan(compare: Int): Boolean = value > compare
}

object CashRecon {
  def main(args: Array[String]): Unit = {
    val transactionTillToday = DaysTrans(List(10, 20, 30, 40))
    val lastReconPosition = Position(100)
    val latestPositionToday = Position(200)
    lastReconPosition plus transactionTillToday equalTo latestPositionToday writeResultToTable
  }
}

case class Position(value: Int) {
  def plus(toDaysTrans: DaysTrans): Sum =
    Sum(value + toDaysTrans.transactions.sum)
}

case class Sum(value: Int) {
  def equalTo(todayPosition: Position): Result =
    new Result(value == todayPosition.value)
}

case class Result(value: Boolean) {
  def writeResultToTable() = {
    println("Recon result ", value)
  }
}

case class DaysTrans(transactions: List[Int]) {}
