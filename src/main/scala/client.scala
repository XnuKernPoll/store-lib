package store_lib.client 
import scodec._, scodec.bits._, codecs.{list => lcodec, utf8}

import com.twitter.util.{Future, Try}
trait Store[K, V]

import store_lib.storage._ 
import Ops._


/**
  A signature for clients with a KV api 

  Clients can use services locally, and you can control how many concurrent local and remote servers, you want to use, and  manage what thread and state resources your servers use. 
*/

trait SimpleKV[K, V] {
  def get(k: K): Future[V]
  def put(k: K, v: V): Future[Unit]
  def delete(k: K): Future[Unit]
  def list: Future[List[K]]
}



/**
 
  Two api clients, for the basic Bucket -> String -> V Rest APIs. 

  Remember this can be done remotely or localy, and it's easy to switch to other protocols, see finagle and twitter util for more details. 
  
*/


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
        val r = Request(Method.Put, (prefix + s"$b/$key" ) )
        r.content(buf); r
      }.flatMap { req =>
        conn(req).map(rep => () )
      }

    }



    def get(key: String): Future[V] = {
      val path = prefix + s"$b/$key"
      val req = Request( Method.Get, path )

      conn(req).map {rep =>
        val buf = BV.fromBuf( rep.content ) 
        val r = valueCodec.decode( buf).toOption.map(x => x.value)
        r.get
      }


    }


    def delete(key: String): Future[Unit] = {
      val path = prefix + s"$b/$key"
      val req = Request( Method.Delete, path)
      conn(req).map( r => () )
    }


    def list: Future[List[String]] = {
      val path = prefix + s"$b"
      val req = Request(Method.Get, path)
      conn(req).map {rep =>
        ( rep.contentString.split(",") ).toList
      }
    }

  }


  class BucketApi(conn: Service[Request, Response] ) {

    val prefix =  "/api/v1/buckets/"
    val base = prefix.dropRight(1)

    def list: Future[List[String]] = {
      val req = Request(Method.Get, base)
      conn(req).map {rep =>
        ( rep.contentString.split(",") ).toList
      }

    }


    def delete(b: String) = {
      val req = Request( Method.Delete, prefix + b)
      conn(req).map {rep => () } 
    }

    def create(b: String) = {
      val req = Request(Method.Post, base)
      req.setContentString(b) 
      conn(req).map {rep => () }
    }


  }


} 
