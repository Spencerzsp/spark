package com.fsnip.scala

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:51 2019/8/2
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */

class PreferredPrompt(val preference: String)
class PreferredDrink(val preferece: String)

object Greeter {

  def  greet(name: String)(implicit prompt: PreferredPrompt, drink: PreferredDrink): Unit = {
    println("Welcome, " + name + ". The system is ready.")
    println("But while you work, ")
    println("why not enjoy a cup of " + drink.preferece + "?")
    println(prompt.preference)
  }
}

object JoesPrefs {
  implicit val prompt = new PreferredPrompt("Yes, master> ")
  implicit val drink = new PreferredDrink("tea")
}
