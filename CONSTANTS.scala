import java.time._

object CONSTANTS {
  //CONSTANT
  val necessary_args = Map(
    "path_data_chain" -> "String",
    "path_data_position" -> "String",
    "output_path" -> "String")

  case class OptionMap(opt: String, value: String) {

    val getOpt: String = opt.split("-") match {
      case (a@Array(_*)) => a.last
      case as: Array[_] => as(0)
      case _ => throw new Exception("Parsing Error")
    }
    val getVal: List[String] = value.split(",").toList.map(_.trim)
  }


  //Parse input arguments from command line.Convert position arguments to named arguments
  def argsPars(args: Array[String], usage: String): collection.mutable.Map[String, List[String]] = {

    if (args.length == 0) {
      throw new Exception(s"Empty argument Array. $usage")
    }

    val (options, _) = args.partition(_.startsWith("-"))
    val interface = collection.mutable.Map[String, List[String]]()
    println(options)

    options.map { elem =>
      val pairUntrust = elem.split("=")
      val pair_trust = pairUntrust match {
        case (p@Array(_, _)) => OptionMap(p(0).trim, p(1).trim)
        case _ => throw new Exception(s"Can not parse $pairUntrust")
      }
      val opt_val = pair_trust.getOpt -> pair_trust.getVal
      interface += opt_val
    }
    interface
  }


  //Check input arguments types
  def argsValid(argsMapped: collection.mutable.Map[String, List[String]]): collection.mutable.Map[String, List[Any]] = {

    val r1 = necessary_args.keys.map(argsMapped.contains(_)).forall(_ == true)

    val validMap = collection.mutable.Map[String, List[Any]]()

    r1 match {
      case true => argsMapped.keys.toList.map { k =>
        necessary_args(k) match {
          case "Long" => try {
            validMap += k -> argsMapped(k).map(_.toLong)
          } catch {
            case _: Throwable => throw new Exception("ERROR")
          }
          case "Boolean" => try {
            validMap += k -> argsMapped(k).map(_.toBoolean)
          } catch {
            case _: Throwable => throw new Exception("ERROR")
          }
          case "String" => try {
            validMap += k -> argsMapped(k).map(_.toString)
          } catch {
            case _: Throwable => throw new Exception("ERROR")
          }
        }
      }
      case false => throw new Exception("Bleat gde argumenti?")
    }
    validMap
  }

  val normalize_value = (arr:Seq[Float]) => {
    val result = arr.map{_ / arr.sum}
    result
  }

  //CONSTANT

}
