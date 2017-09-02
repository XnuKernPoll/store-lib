package store_lib 
import server.Handlers._
import client._
import org.scalatest._
import storage.State.BucketMap
import scodec._, codecs.{utf8}
import com.twitter.util._


class svcSuite extends FunSuite {
  import StringDict._ 
  val rsrcs = Resources.zero 
  val srvr = Server(rsrcs)

  val bucketAPI = new BucketApi(srvr) 

 
  test ("Create and List") {
    Await.result( bucketAPI.create("buddehs") )
    assert { rsrcs.bm.contains("buddehs") }
    val l = Await.result( bucketAPI.list)
    assert{ l.sorted == (rsrcs.bm.keys).toList.sorted } 
    
  }


  

  val kvAPI = new RemoteKV[String]("buddehs", utf8, srvr)

  test ("Put and Get") {
    val canadians = "I am not yo buddeh fwend, eat me guy"
    val p = kvAPI.put("arguing", canadians)
  }


  test ("put, list, delete") {
    val kvs = List(
      ("Cartman", "Respect mah authoratah"),
      ("Stan", "I am a ten year old alcoholic gambler"),
      ("Kyle", "I like giving libretarian speeches"),
      ("Kenny", "I sucked off Howard Stern, for 50 bucks and am a superhero")
    )

    val c = Future.collect { kvs.map(x => kvAPI.put(x._1, x._2)  ) }
    Await.result(c)
    val keys = kvs.map(x => x._1) :+ "arguing"
    assert { ( Await.result(kvAPI.list) ).sorted == keys.sorted }

    Await.result( kvAPI.delete("Cartman") )
    assert( Await.result(kvAPI.list).contains("Cartman") != true  ) 
  }


  test ("Destroy ") {
    Await.result( bucketAPI.delete("buddehs") )
    assert {rsrcs.bm.contains("buddehs") != true } 
    
  }


}
