package fare.nyctaxi.jobs

import fare.nyctaxi.args.Parameters
import fare.nyctaxi.treatment.TreatmentJob
import fare.nyctaxi.consumer.ConsumerJob
import fare.nyctaxi.producer.ProducerJob

object MainScript {
  def main(args: Array[String]): Unit = {
    val parameters = Parameters.parse(args)

    parameters.job match {
      case "treatment"   => TreatmentJob.main(Array())
      case "consumer"    => ConsumerJob.main(Array())
      case "producer"    => ProducerJob.main(Array())
      case _             => throw new IllegalArgumentException(s"Unknown job: ${parameters.job}")
    }
  }
}
