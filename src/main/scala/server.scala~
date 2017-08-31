package server
import com.twitter.{util => Util, io => IO , finagle => Finagle} 
import org.iq80.leveldb.Options
import store.KV.BucketMap, store.BlockStore.Bucket, store.{util => store_util}


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


  // add list 

}


object kv_api {
  import IO.Buf.ByteArray.Shared

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
