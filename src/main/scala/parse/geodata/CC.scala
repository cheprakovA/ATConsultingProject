package parse.geodata

class CC[T] {
  def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T])
}

object M extends CC[Map[String, Any]]
object S extends CC[String]
object L extends CC[List[Any]]
object D extends CC[Double]
object LL extends CC[List[List[Any]]]
object LD extends CC[List[Double]]