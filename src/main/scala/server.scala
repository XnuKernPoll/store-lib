package server


import com.twitter.{util => Util, io => IO , finagle => Finagle} 
import org.iq80.leveldb.Options
import store.{util => store_util}, store._


import scodec.bits._
import scodec._, codecs.{list => listEncode, utf8}

import State._ 
import Finagle.http.{Request, Response, Status, Method}, Util.{Future, Try}



import Finagle.http.path._, Finagle.Service, Finagle.Failure

object BucketApi {

  import Ops._ 
  def create(buckets: BucketMap, bpath: String): Future[Response] = Future.const { BucketMap.mkBucket(buckets, bpath) }.map {b =>
    val r = Response(Status.Ok)
    r.setContentString(s"Added ${bpath} to store.")
    r
  }


  def delete(buckets: BucketMap, bpath: String): Future[Response] = Future.value { BucketMap.rmBucket(buckets, bpath) }.map {r =>
    val rep = Response(Status.Ok)
    rep.setContentString(s" removed $bpath from store. ")
    rep
  }


  def list(buckets: BucketMap): Future[Response] = {
    val k = buckets.keys.toList
    val pl = listEncode(utf8).encode(k).toOption.map { data =>
      store_util.BV.toBuf(data) 
    }

    Future.const { store_util.optAsTry(pl) }.map { data =>
      val rep = Response(Status.Ok)
      rep.content(data); rep
    }



  }



}


object kv_api {
  import IO.Buf.ByteArray.Shared

  import Ops._, State._ 

  def list(bm: BucketMap, path: String) = {
    def tx = (b: Bucket) => KV.list(b)

    performOP(bm, path, tx).flatMap {d =>

      val pl = store_util.optAsTry( listEncode(utf8).encode(d).toOption.map { data => store_util.BV.toBuf(data)  } )

      Future.const {pl}.map { data =>
        val rep = Response(Status.Ok)
        rep.content(data); rep
      }


    }


  }

  def get(s: BucketMap, path: String, key: Array[Byte]) = {
    val tx = (b: Bucket) =>  store_util.optAsTry(  KV.get(b, key) )

    performOP(s, path, tx).map { d =>
      val rep = Response(Status.Ok) 
      val buf = store_util.asBuf(d)
      rep.content(buf); rep 
    }

  }


  def performOP[T](bmap: BucketMap, path: String, op: Bucket => Try[T] ): Future[T] = {
    val t = store_util.optAsTry( BucketMap.getBucket(bmap, path) )
    Future.const(t) flatMap { b => Future.const( op(b) ) }
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

  def KV(bm: BucketMap) = Service.mk[Request, Response] { req => (req.method, Path(req.path)  ) match {

    //Delete 
    case Method.Delete -> root / "api" / "v1" / "kv" / b / k => kv_api.delete(bm, b, k.getBytes)

    //Put 
    case Method.Put -> root / "api" / "v1" / "kv" / b / k =>
      val v = Shared.extract( req.content )
      kv_api.put(bm, b, k.getBytes, v) 

    //Get 
    case Method.Get -> root / "api" / "v1" / "kv" / b / k  => kv_api.get(bm, b, k.getBytes)


    //List 
    case Method.Get -> root / "api" / "v1" / "kv" /  b  => kv_api.list(bm, b)
  } }

   

  def Buckets(bm: BucketMap) = Service.mk[Request, Response] { req => (req.method, Path(req.path) ) match {

    case Method.Post -> root / "api" / "v1" /  "buckets" =>
      val p = req.contentString
      BucketApi.create(bm, p)

    case Method.Get -> root / "api" / "v1" / "buckets" => BucketApi.list(bm)

    case Method.Delete -> root / "api" / "v1" / "buckets" / p => BucketApi.delete(bm, p)

  } } 




}

