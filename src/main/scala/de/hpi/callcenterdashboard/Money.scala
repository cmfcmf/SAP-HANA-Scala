package de.hpi.callcenterdashboard

case class Money(amount: BigDecimal, currency: String) {
  def +(money: Money): Money = {
    if (currency != money.currency) {
      throw new Exception("Can only add money with the same currency.")
    }
    Money(amount + money.amount, currency)
  }
  def -(money: Money): Money = {
    if (currency != money.currency) {
      throw new Exception("Can only subtract money with the same currency.")
    }
    Money(amount - money.amount, currency)
  }
  def amountAsString: String = amount.toString()
  override def toString: String = currency + " " + amount.toString()
}
