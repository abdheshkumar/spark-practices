import AkkAExample.Fail
import akka.actor.{Actor, ActorSystem, Props}

import scala.util.Random

object AkkAExample extends App {

  case object Fail

  val system = ActorSystem("test")
  val r = Random.nextInt()
  val actor = system.actorOf(Props(new MyActor(Random.nextInt())))
  actor ! "Test"
  actor ! Fail
}

class MyActor(arg: => Int) extends Actor {
  println("Constructor::" + arg)

  override def receive: Receive = {
    case a: String => println(":::a")
    case Fail => throw new Exception("manually throw an exception")
  }
}