package fare.nyctaxi.args

case class Parameters(
      job: String = "",
      cloudMode: Boolean = false
)

object Parameters {

  def parse(args: Array[String]): Parameters = {
    new scopt.OptionParser[Parameters]("batchprocessing") {
      head("batchprocessing")
      help('h', "help").text("""
                               | This module runs for the selected book.
                               | The book titulos is used only for PF
                               |""".stripMargin)
      opt[String]("job")
        .action(
          (job, params) => {
            params.copy(job = job)
          }
        )
        .text(
          """
            | String that defines which job step needs to be executed (required)
            | Options: preprocessingjob, processingjob, posprocessingjob
            |""".stripMargin
        )
        .required()
    }.parse(args, Parameters()) match {
      case Some(parameters) => parameters
      case _                => throw new IllegalArgumentException()
    }
  }

}