package com.wikibooks.spark.ch5.scala

case class Person(name: String, age: Int, job: String)

class Person2(
  name: String,
  age:  Int,
  job:  String,
  v3:   String = "-",
  v4:   String = "-",
  v5:   String = "-",
  v6:   String = "-",
  v7:   String = "-",
  v8:   String = "-",
  v9:   String = "-",
  v10:  String = "-",
  v11:  String = "-",
  v12:  String = "-",
  v13:  String = "-",
  v14:  String = "-",
  v15:  String = "-",
  v16:  String = "-",
  v17:  String = "-",
  v18:  String = "-",
  v19:  String = "-",
  v20:  String = "-",
  v21:  String = "-",
  v22:  String = "-",
  v23:  String = "-",
  v24:  String = "-",
  v25:  String = "-",
  v26:  String = "-",
  v27:  String = "-",
  v28:  String = "-",
  v29:  String = "-")
  extends java.io.Serializable
  with Product {
  def canEqual(that: Any) = that.isInstanceOf[Person2]
  def productArity = 30
  def productElement(idx: Int) = idx match {
    case 0  => name
    case 1  => age
    case 2  => job
    case 3  => v3
    case 4  => v4
    case 5  => v5
    case 6  => v6
    case 7  => v7
    case 8  => v8
    case 9  => v9
    case 10 => v10
    case 11 => v11
    case 12 => v12
    case 13 => v13
    case 14 => v14
    case 15 => v15
    case 16 => v16
    case 17 => v17
    case 18 => v18
    case 19 => v19
    case 20 => v20
    case 21 => v21
    case 22 => v22
    case 23 => v23
    case 24 => v24
    case 25 => v25
    case 26 => v26
    case 27 => v27
    case 28 => v28
    case 29 => v29
  }
}