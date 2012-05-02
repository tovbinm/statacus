package statacus

import scala.util.control.Exception.allCatch
import scala.util.Random
import java.net.{InetAddress => Addr, InetSocketAddress => SocketAddr}
import java.nio.channels.DatagramChannel
import java.nio.ByteBuffer


trait Pusher[T] {
  def push(addr: SocketAddr, chan: DatagramChannel, stats: String*): Seq[T]
}

object Pusher {
  type BytesSent = Either[String, Int]
  implicit object DefaultPusher extends Pusher[BytesSent] {
    def push(addr: SocketAddr, chan: DatagramChannel, stats: String*) =
      for(stat <- stats) yield (allCatch.opt {      
        chan.send(ByteBuffer.wrap(stat.getBytes), addr) match {
          case sent if(sent != stat.getBytes.size) => Left(
            "%s was only partially transmitted".format(stat, addr.getHostName, addr.getPort))
          case sent => Right(sent)
        } }).getOrElse(
          Left("exception thrown while transmitting %s".format(stat))
        )
  }
}

// see https://github.com/etsy/statsd#readme
class Statsd(host: String, port: Int = 8125, prefix: String = "") {
  val addr = new SocketAddr(Addr.getByName(host), port)
  val chan = DatagramChannel.open
  val rand = new Random
  val prefix_str = if (prefix!=null && prefix.length>0 && prefix.endsWith(".")) prefix 
    			         else { prefix + "." } 

  def counter[T](key: String, by: Int = 1, rate: Double = 1.0)(implicit p: Pusher[T]) = new {
	 var c : Long = 0L
     def inc = { c = c + by; Statsd.this.inc(key, by, rate) }
     def dec = { c = c - by; Statsd.this.dec(key, by, rate) }
     def total = c
  }

  def inc[T](key: String, by: Int = 1, rate: Double = 1.0)(implicit p: Pusher[T]) =
    count(if(by < 0) -by else by, rate)(key)(p)

  def dec[T](key: String, by: Int = -1, rate: Double = 1.0)(implicit p: Pusher[T]) =
    count(if(by < 0) by else -by, rate)(key)(p)

  def count[T](by: Int = 1, rate: Double = 1.0)(keys: String*)(implicit p: Pusher[T]) =
    sample(rate, (for(k <- keys) yield "%s%s:%d|c".format(prefix_str,k, by)): _*)(p)

  def time[T](key: String,`val`: Int, rate: Double = 1.0)(implicit p: Pusher[T]) =
    sample(rate, "%s%s:%d|ms".format(prefix_str, key, `val`))(p)

  private def sample[T](rate: Double, stats: String*)(implicit pusher: Pusher[T]) =
    if(rate >= 1.0) pusher.push(addr, chan, stats: _*)
    else pusher.push(addr, chan,
        (for(stat <- stats; d <- Some(rand.nextDouble) if (d <= rate)) yield "%s|@%f".format(stat, rate)): _*)
}