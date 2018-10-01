case class Product(name: String, id: Int) {
}

object Demo {
  def main(args: Array[String]): Unit = {
    val products = List(Product("Mahadev", 1), Product("Hello", 2), Product("Mahadev", 3), Product("Hello", 4))
    products.sortBy(_.name).foreach(x => {
      println(x.name)
    })

    products.map(_.name.toUpperCase).foreach(x => {
      println(x)
    })

    val result: Map[Char, List[Product]] = products.groupBy(_.name.head)
    result.keys.foreach(x => {
      println(x)
      result.get(x).foreach(l => {

      })
    })
  }
}
