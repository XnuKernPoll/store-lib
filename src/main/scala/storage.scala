package storage
import scodec._
import scodec.bits._, codecs._

import com.twitter.util.{Try, Return, FuturePool, Future}
import java.util.concurrent.ForkJoinPool 
import org.iq80.leveldb._


import scala.annotation.tailrec


import org.fusesource.leveldbjni.JniDBFactory._

//add write ahead cache

object util {
  import com.twitter.io.Buf

  def optAsTry[T](opt: Option[T] ): Try[T] = Try {
    opt match {
      case Some(x) => x
      case None => throw ( new Exception("Null Data") )
    }
  }

   def configure(block_size: Int = 512, num_entries: Int = 1000): Options = {
     val opt = new Options()
     opt.cacheSize(block_size * num_entries)
     opt.blockSize(num_entries)
     opt.createIfMissing(true)
     opt
   }



  object Buffers { 
    def asBuf(b: Array[Byte])  = Buf.ByteArray.Shared(b)
    def toBytes(buf: Buf) = Buf.ByteArray.Shared.extract(buf)
  }

  object BV {

    def fromBuf(buf: Buf) = {
      val bytes = Buf.ByteArray.Shared.extract(buf)
      BitVector(bytes)
    }


    def toBuf(buf: BitVector)  = Buffers.asBuf( buf.toByteArray ) 
  }


}


object Ops {

  import java.nio.file.{Files => FileModule, Paths}
  import java.io.File, com.google.common.io.Files

  case class Bucket(path: String, db: DB, pool: FuturePool, opts: Options )


  object Bucket {




    def make(path: String , pool: FuturePool, opt: Options): Future[Bucket] = pool {
      val fp = new File(path)
      if (opt.createIfMissing() != true ) opt.createIfMissing(true)  
      val db = factory.open(fp, opt)
      Bucket( path, db ,pool , opt)
    }

    def destroy(bckt: Bucket): Future[Unit] = bckt.pool {  bckt.db.close(); factory.destroy(new File(bckt.path), bckt.opts ) } 

  }




  object KV {
    

    def get(b: Bucket, key: Array[Byte]): Future[ Array[Byte] ]  = b.pool { b.db.get(key) }


    def put(b: Bucket, key: Array[Byte], value: Array[Byte] ): Future[Unit] = b.pool{ b.db.put(key, value) }


    def delete(b: Bucket, key: Array[Byte]): Future[Unit] = b.pool { b.db.delete(key) }

    def list(b: Bucket) = {
      val iter = synchronized { b.db.iterator() } 
      iter.seekToFirst()

      @tailrec def populate(i: DBIterator, keys: List[String]): Future[ List[String] ]  = {
        val key = asString( i.next().getKey() ).toString 
        val ukeys = (keys :+ key ).toList
        
        if ( iter.hasNext() ) populate(i, ukeys) else {
          synchronized { i.close() }
          Future( ukeys )
        }


      }

      populate(iter, List[String]() ) 
    }

  }

}


object State { 

  import Ops._
  import scala.collection.concurrent.TrieMap

  type BucketMap = TrieMap[String, Bucket]


  object BucketMap {
    //destroy

    def make = new BucketMap()

    def mkBucket(bckts: BucketMap, pool: FuturePool, p: String) = {
      val opts = util.configure()
      val b = Bucket.make(p, pool, opts)
      b.map {b =>
        bckts += (p -> b)
      }
    }

    def rmBucket(bckts: BucketMap, p: String): Try[Unit] = Try {

      bckts.get(p) match {
        case Some(b) =>
          Bucket.destroy(b)
          bckts -= p 

        case None => () 
      }


    }

    def getBucket(bckts: BucketMap, p: String): Option[Bucket] = { bckts.get(p) }

  }
}
