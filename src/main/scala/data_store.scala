package access 
import scodec._, scodec.bits._, codecs.{list => lcodec, utf8}

import com.twitter.util.{Future, Try}
trait Store[K, V]

import storage._ 
import Ops._

trait SimpleKV[K, V] {
  def get(k: K): Future[V]
  def put(k: K, v: V): Future[Unit]
  def delete(k: K): Future[Unit]
  def list: Future[List[K]]
}


object StringDict { 


  import util.BV
  import com.twitter.finagle.{Service, http}
  import http.{Request, Response, Method}


  class RemoteKV[V](b: String, valueCodec: Codec[V], conn: Service[Request, Response]) extends SimpleKV[String, V] {
    val prefix = "/api/v1/kv/"


    def put(key: String, v: V): Future[Unit] = {
      val payload: Try[BitVector] = util.optAsTry( valueCodec.encode(v).toOption )

      Future.const(payload).map { data =>
        val buf = BV.toBuf(data)
        //sanitize later
        val r = Request(Method.Put, (prefix + s"/$b/$key" ) )
        r.content(buf); r
      }.flatMap { req =>
        conn(req).map(rep => () )
      }

    }



    def get(key: String): Future[V] = {
      val path = prefix + s"/$b/$key"
      val req = Request( Method.Get, path )

      conn(req).map {rep =>
        val buf = BV.fromBuf( rep.content ) 
        val r = valueCodec.decode( buf).toOption.map(x => x.value)
        r.get
      }


    }


    def delete(key: String): Future[Unit] = {
      val path = prefix + s"/$b/$key"
      val req = Request( Method.Delete, path)
      conn(req).map( r => () )
    }


    def list: Future[List[String]] = {
      val path = prefix + s"/$b"
      val req = Request(Method.Get, b)
      conn(req).map {rep =>
        val buf = BV.fromBuf(  rep.content )
        lcodec( utf8 ).decode(buf).toOption.map(x => x.value).get   
      }
    }


 

    


  }



} 
