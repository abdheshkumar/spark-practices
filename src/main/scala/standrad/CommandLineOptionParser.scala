package standrad

import scopt.OptionParser

case class UsageConfig(date: String = "")

class CommandLineOptionParser
  extends OptionParser[UsageConfig]("job config") {
  head("scopt", "3.x")

  opt[String]('d', "date").required
    .action((value, arg) => {
      arg.copy(date = value)
    })
    .text("The date")
}