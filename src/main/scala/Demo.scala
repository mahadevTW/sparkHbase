object DemoDQ {
  def main(args: Array[String]): Unit = {
    val marketValue = MarketValue(50)
    val isValidMarketValue = marketValue isNotZero
    val thresholdValue = 100
    val isGreaterThan = marketValue isGreaterThan thresholdValue
    println(isValidMarketValue)
    println(isGreaterThan)
  }
}
