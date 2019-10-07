val n = 8
val lst = List(
  List(1.0,11.0), List(2.0,22.0),
  List(3.0,33.0), List(4.0,44.0))

def take(k: Int, row: (Int, List[List[Any]])): Double = row._2
  .drop((k - 1) / 2)
  .head
  .dropRight(k % 2)
  .last
match {
  case d: Double => d.toDouble
  case _ => 0.0
}
take(n, (1, lst))