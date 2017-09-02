
package storage

import org.scalatest.{FunSuite}
import java.io.File
import com.twitter.util._



object Helpers {
  def assertOption[T](o: Option[T]) = o match {case Some(v) => true; case None => false}

  def assertFuture[T](f: Future[T]) = f.poll match {
    case Some(x) => x.isReturn
    case _ => false 
  } 

  def assertTry[T](t: Try[T]) = t.isReturn


}




class OpsSuite extends FunSuite {
  import Ops._ , Helpers._

  val pool = FuturePool.unboundedPool
  val b = Await.result( Bucket.make("pals", pool, util.configure()  ) )

  val (k, v) = ("buddy".getBytes, "guy".getBytes)


  test ( "Put and Get " ) {  
    Await.result( KV.put(b, k, v) )
    val res = KV.get(b, k)
    assertFuture( res )
    assert( new String( Await.result(res) ) == new String( v ) ) 
  }


  test ("Delete and Get ")  {
    Await.result(KV.delete(b, k) )
    assert { assertFuture( KV.get(b, k) ) != true }
  }

  test("put and list") {
    val t = Await.result( KV.put(b, k, v) )
    val l = KV.list(b)
    assert { assertFuture(l) }
    assert { Await.result(l) == List(new String(k) )  } 
  }


  test ("Destroy") {
    val op = Bucket.destroy(b)
    Await.result( op )
    assert { assertFuture(op) }
    assert { (new File("pals") ).exists() != true }  
  }
 
}
