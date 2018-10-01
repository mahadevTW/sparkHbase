object Set {
  def field(name: String): Of = new Of(name)
}

class Of(val name: String) {
  def of(obj: AnyRef): To = new To(name, obj)
}

class To(name: String, obj: AnyRef) {
  def to(value: Any): Unit = {
    val f = obj.getClass.getDeclaredField(name)
    f.setAccessible(true)
    f.set(obj, value)
  }
}

object DemoDSl {
  def main(args: Array[String]): Unit = {
    //    Set field “foo” of bar to “baz”
    val bar = new Bar

    Set field "Hello" of bar to "baz"
  }
}

class Bar