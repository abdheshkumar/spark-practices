import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, SourceShape}
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{Future, Promise}
object MeetupApp extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // using Source.maybe materializes into a promise
  // which will allow us to complete the source later
  import system.dispatcher

  val kafkaProducerSettings =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")
  val kafkaSink = Producer.plainSink(kafkaProducerSettings)
  val printSink =
    Flow[Message].mapConcat {
      case tm: TextMessage.Strict =>
        new ProducerRecord[String, String]("test", tm.text) :: Nil
      case bm: BinaryMessage =>
        // ignore binary messages but drain content to avoid the stream being clogged
        bm.dataStream.runWith(Sink.ignore)
        Nil
      case bm: TextMessage =>
        // ignore binary messages but drain content to avoid the stream being clogged
        bm.textStream.runWith(Sink.ignore)
        Nil
    }.toMat(kafkaSink)(Keep.right)

  val flow: Flow[Message, Message, Promise[Option[Message]]] =
    Flow.fromSinkAndSourceMat(
      printSink,
      Source.maybe[Message]
    )(Keep.right)

  val (upgradeResponse, promise) =
    Http().singleWebSocketRequest(
      WebSocketRequest("ws://stream.meetup.com/2/rsvps"),
      flow
    )
  val connected = upgradeResponse.flatMap { upgrade =>
    if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
      println("Connection Established..")
      Future.successful(Done)
    } else {
      throw new RuntimeException(
        s"Connection failed: ${upgrade.response.status}"
      )
    }
  }

  val cs: CoordinatedShutdown = CoordinatedShutdown(system)
  cs.addTask(
    CoordinatedShutdown.PhaseServiceStop,
    "shut-down-client-http-pool"
  )(() => {
    Http()(system).shutdownAllConnectionPools().map(_ => {
      println("Stopping......")
      promise.success(None) //// at some later time we want to disconnect
      Done
    })
  })
  connected.onComplete { tr =>

    println(tr)
  }
}
