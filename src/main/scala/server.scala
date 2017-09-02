package server

import com.twitter.{util => Util, io => IO , finagle => Finagle} 
import org.iq80.leveldb.Options

import storage.{util => store_util}, storage._
import scodec.bits._
import scodec._, codecs.{list => listEncode, utf8}

import State._ 
import Finagle.http.{Request, Response, Status, Method}, Util.{Future, Try}
import java.util.concurrent.{ForkJoinPool}, Util.{FuturePool, ExecutorServiceFuturePool}


import Finagle.http.path._, Finagle.Service, Finagle.Failure

object BucketApi {

  import Ops._ 
  def create(buckets: BucketMap, p: FuturePool,  bpath: String): Future[Response] = BucketMap.mkBucket(buckets, p ,  bpath).map {b =>
    val r = Response(Status.Ok)
    r.setContentString(s"Added ${bpath} to store.")
    r
  }


  def delete(buckets: BucketMap, bpath: String): Future[Response] = Future.value { BucketMap.rmBucket(buckets, bpath) }.map {r =>
    val rep = Response(Status.Ok)
    rep.setContentString(s" removed $bpath from store. ")
    rep
  }


  def list(buckets: BucketMap): Future[Response] = Future {
    val k = buckets.keys.toList
    val pl = k.mkString(",")

    val rep = Response(Status.Ok)
    rep.setContentString(pl); rep
 
  }



}


object kv_api {
  import IO.Buf.ByteArray.Shared

  import Ops._, State._ 

  def list(bm: BucketMap, path: String) = {
    def tx = (b: Bucket) => KV.list(b)

    performOP(bm, path, tx).map { l =>
      val pl = l.mkString(",") 
      val response = Response(Status.Ok)
      response.setContentString(pl); response
    }

  }

  def get(s: BucketMap, path: String, key: Array[Byte]) = {
    val tx = (b: Bucket) => KV.get(b, key) 

    performOP(s, path, tx).map { d =>
      val rep = Response(Status.Ok) 
      val buf = store_util.Buffers.asBuf(d)
      rep.content(buf); rep 
    }

  }


  def performOP[T](bmap: BucketMap, path: String, op: Bucket => Future[T] ): Future[T] = {
    val t = store_util.optAsTry( BucketMap.getBucket(bmap, path) )
    Future.const(t) flatMap { b => op(b)  }
  }


  def put(bmap: BucketMap, path: String, key: Array[Byte], value: Array[Byte]) = {
    val p = (b: Bucket) => { KV.put(b, key, value) }

    performOP(bmap, path, p).map { x =>
      val rep = Response( Status.Ok )
      rep.setContentString( "Key has been set" ); rep 
    }

  }


  def delete(bmap: BucketMap, path: String, key: Array[Byte]) = {
    val tx = (b: Bucket) => { KV.delete(b, key) }
    performOP(bmap, path, tx) map { d =>
      val rep = Response( Status.Ok )
      rep.setContentString( "key has been deleted" ); rep 
    }
  }


}



object Handlers {

  import IO.Buf.ByteArray.Shared

  case class Resources(bm: BucketMap, pool: FuturePool)


  object Resources {
    def zero: Resources = Resources(new BucketMap(), defaultPool)
  }


  def defaultPool = {
    val fjp = new ForkJoinPool(6) 
    new ExecutorServiceFuturePool(fjp) 
  }



  def KV(bm: BucketMap) = Service.mk[Request, Response] { req => (req.method, Path(req.path)  ) match {

    //Delete 
    case Method.Delete -> root / "api" / "v1" / "kv" / b / k => kv_api.delete(bm, b, k.getBytes)

    //Put 
    case Method.Put -> root / "api" / "v1" / "kv" / b / k =>
      val v = Shared.extract( req.content )
      kv_api.put(bm, b, k.getBytes, v)

     //list
    case Method.Get -> root / "api" / "v1" / "kv" / b => kv_api.list(bm, b)

    //Get 
    case Method.Get -> root / "api" / "v1" / "kv" / b / k  => kv_api.get(bm, b, k.getBytes)


    //List 

  } }

   

  def Buckets(r: Resources) = Service.mk[Request, Response] { req => (req.method, Path(req.path) ) match {

    case Method.Post -> root / "api" / "v1" /  "buckets" =>
      val p = req.contentString
      BucketApi.create(r.bm, r.pool, p)

    case Method.Get -> root / "api" / "v1" / "buckets" => BucketApi.list(r.bm)

    case Method.Delete -> root / "api" / "v1" / "buckets" / p => BucketApi.delete(r.bm, p)

  } } 

  import Finagle.http.HttpMuxer

  def Server(r: Resources) = {
    val bm = r.bm 
    val kv = KV(bm)
    val buckets = Buckets(r)

    val muxer = new HttpMuxer()
      .withHandler("/api/v1/kv/", kv)
      .withHandler("/api/v1/buckets", buckets)
      .withHandler("/api/v1/buckets/", buckets)


    muxer
  }


}

