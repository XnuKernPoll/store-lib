package server


import com.twitter.{util => Util, io => IO , finagle => Finagle} 
import org.iq80.leveldb.Options
import store.KV.BucketMap, store.BlockStore.Bucket, store.{util => store_util}

import scodec._, codecs._
import scodec.bits._

import Finagle.http.{Request, Response, Status}, Util.{Future, Try}



object BucketApi { 

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


  def list(buckets; BucketMap) = Future {
    val k = buckets.keys
    val pl = list(utf8).encode(k).toOption.map( bv => store_util.asBuf(bv.toByteArray) )
    val rep = Response(Status.Ok)

    pl match {
      case Some(res) => rep.setContentString(res); Future(rep)
      case None => Future.exception( Failure.rejected("couldn't marshal bucket names").asNonRetryable ) 
    }

  }

}


object kv_api {
  import IO.Buf.ByteArray.Shared

  def list(bm: BucketMap, path: String) = {
    Bucket.get(b, key)
    store_util.optAsTry(b, key) 
  }

  def get(s: BucketMap, path: String, key: Array[Byte]) = {
    val tx = (b: Bucket) =>  store_util.optAsTry(  Bucket.get(b, key) )

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
    val p = (b: Bucket) => { Bucket.put(b, key, value) }

    performOP(bmap, path, p).map { x =>
      val rep = Response( Status.Ok )
      rep.setContentString( "Key has been set" ); rep 
    }

  }


  def delete(bmap: BucketMap, path: String, key: Array[Byte]) = {
    val tx = (b: Bucket) => { Bucket.delete(b, key) }
    performOP(bmap, path, tx) map { d =>
      val rep = Response( Status.Ok )
      rep.setContentString( "key has been deleted" ); rep 
    }
  }


}



object Handlers {

  /* 
    TODO switch to their own endpoints 
    Method   /api/v1/kv/:bucket 
    Method /api/v1/buckets/:bucket 
    
   */



  def KV(b: BucketMap) = Service[Request, Response].mk { req => (req.method, Path(req.path)  ) match {

    //Delete 
    case Method.Delete -> root / "api" / "v1" / "buckets" / b / k => kv_api.delete(bm, b, k.getBytes)

    //Put 
    case Method.Put -> root / "api" / "v1" / "buckets" / b / k =>
      val v = Shared.extract( req.content )
      kv_api.get(bm, b, k.getBytes, v) 

    //Get 
    case Method.Get -> root / "api" / "v1" / "kv" / b / k  => kv_api.get(bm, b, k.getBytes)


    //List 
    case Method.Get -> root / "api" / "v1" / "kv" /  b  => kv_api.list(bm)
  } }
}

class ApiHandler(bm: BucketMap) extends Service[Request, Response] {
  import IO.Buf.ByteArray.Shared

  def apply(req: Request) = (req.method, Path(req.path)) match {

    case Method.Post -> root / "api" / "v1" /  "buckets" =>
      val p = req.contentString
      BucketApi.create(bm, p)

    case Method.Get -> root / "api" / "v1" / "buckets" => BucketApi.list(bm)

    case Method.Delete -> root / "api" / "v1" / "buckets" / p => BucketApi.delete(bm, p)

  }
}
