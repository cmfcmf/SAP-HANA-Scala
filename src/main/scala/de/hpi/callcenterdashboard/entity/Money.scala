package de.hpi.callcenterdashboard.entity

import java.text.NumberFormat
import java.util.Locale

/**
  * Represents a particular amount of money with currency.
  *
  * @param amount The amount of money.
  * @param currency The currency identifier.
  */
case class Money(amount: BigDecimal, currency: String) {
  /**
    * Sum up the amounts and return a new object. Throws an error if the currencies don't match.
    *
    * @param money The money to sum up.
    * @return
    */
  def +(money: Money): Money = {
    if (currency != money.currency) {
      throw new Exception("Can only add money with the same currency.")
    }
    Money(amount + money.amount, currency)
  }

  /**
    * Subtract the given amount and return a new object. Throws an error if the currencies don't match.
    *
    * @param money The money to sum up.
    * @return
    */
  def -(money: Money): Money = {
    if (currency != money.currency) {
      throw new Exception("Can only subtract money with the same currency.")
    }
    Money(amount - money.amount, currency)
  }

  /**
    * Returns the amount as unformatted string. Useful for JavaScript.
    *
    * @return
    */
  def amountAsString: String = amount.toString()

  /**
    * Creates a string with currency and formatted amount.
    *
    * @return
    */
  override def toString: String = {
    val numberFormat = NumberFormat.getInstance(Locale.US)
    numberFormat.setMinimumFractionDigits(2)
    numberFormat.setMaximumFractionDigits(2)

    s"$currency " + numberFormat.format(amount)
  }
}
