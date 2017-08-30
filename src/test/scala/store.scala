package store 
import org.scalatest.{FunSuite}
import java.io.File
import com.twitter.util._
import org.fusesource.leveldbjni.JniDBFactory._

object Helpers {
  def assertOption[T](o: Option[T]) = o match {case Some(v) => true; case None => false} 
}

class BlockStoreTest extends FunSuite {
  import BlockStore._
  import java.nio.file.{Files, Paths}
  
  val opts = util.configure()

  test ("opt gen") { assert( opts.createIfMissing() ) }
   

  val p = "/home/sam/fucknigga.db"
  //val d = Try { factory.destroy(new File(p), opts ) } 
  val b = Bucket.make(p, opts)

  test ("bucket creation") {
    assert { Files.exists( Paths.get(p) ) }
    assert (b.isReturn )
  }

  test ("CRUD Operations") {
    val bckt = b.get 
    val t = Bucket.put(bckt, "n".getBytes, "coon".getBytes)
    assert(t.isReturn)

    val g = Bucket.get(bckt, "n".getBytes)
    val gb = g match {case Some(res) => true ; case None => false}

    assert(gb)
    assert( ( new String(g.get) ).trim()  == "coon" ) 

    val del = Bucket.delete(bckt, "n".getBytes)
    assert(del.isReturn)
  }


  test ("bucket destruction") {
    val del = Bucket.destroy(b.get)
    assert(del.isReturn) 
  }

}


class  BucketMapTest extends FunSuite {
  import KV.BucketMap

  val tbl = BucketMap.make

  test ("CRUD") {
    val bm = BucketMap.mkBucket(tbl, "slut")
    assert {bm.isReturn }

    val bck = BucketMap.getBucket(tbl, "slut")
    assert { Helpers.assertOption(bck) }

    val del = BucketMap.rmBucket(tbl, "slut")
    assert(del.isReturn)
  } 

}
