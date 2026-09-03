object Utils {

  def parseCsvLine(line: String): Array[String] = {
    line.split(",", -1).map(_.trim)
  }

  def isValidEventType(eventType: String): Boolean = {
    eventType == "SALE" || eventType == "RESTOCK"
  }

  def safeInt(value: String, defaultValue: Int = 0): Int = {
    try {
      value.trim.toInt
    } catch {
      case _: NumberFormatException => defaultValue
    }
  }

  def printSection(title: String): Unit = {
    println()
    println("=" * 70)
    println(title)
    println("=" * 70)
  }
}
