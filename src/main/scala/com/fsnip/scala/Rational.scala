package com.fsnip.scala

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 14:12 2019/8/2
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
class Rational(n: Int, d: Int) {

  require(d != 0)
  private val g = gcd(n.abs, d.abs)
  val numer: Int = n / g
  val denom: Int = d / g

  def this(n: Int) = this(n, 1) //辅助构造器

  override def toString: String = numer + "/" + denom

  def + (that: Rational): Rational = {
    new Rational(numer * that.denom + that.numer * denom, denom * that.denom)
  }

  def + (i: Int): Rational = {
    new Rational(numer + i * denom, denom)
  }

  def * (that: Rational): Rational = {
    new Rational(numer * that.numer, denom * that.denom)
  }

  def * (i: Int): Rational = {
    new Rational(numer * i, denom)
  }

  private def gcd(a: Int, b: Int): Int = {
    if (b == 0)
      a
    else
      gcd(b, a % b)
  }

}
