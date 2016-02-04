import scala.util.Random
import scalax.io._
import resource.{managed}

/**
  * User:      kaa
  * Timestamp: 02/02/16 15:38
  */
object ReservoirSampling {

  def generateFirstPos(ss: DBSession, res: Array[Position], docLimit : Long): (Int, Int) = {

    val k = res.length
    var (i, j) = (0, 0)

    while(i < k) {

      var step = 100

      ss.GetDocRecord(i % docLimit) map {
        dr =>
          val c = dr.count

          j = 0
          while (i + j < k && j <= c) {
            res(i + j) = Position.mkFromFoc(dr, j + 1)
            j += 1
          }

          step = c
      }

      i += step
    }

    if (i > 0 && j > 0)
      (i-1, j-1)
    else
      (i, j)
  }

  def sample(ss: DBSession, res: Array[Position], dlim : Long) = {

    val k = res.length
    val lastDoc  = Math.min(ss.lastDocId, dlim)
    val totalPos = ss.totalPosCount

    var posSoFar = 0L

    val (startFromDoc, _) = generateFirstPos(ss, res, lastDoc)

    posSoFar = 0L
    for (d <- startFromDoc.toLong to lastDoc) {

      var step = 10

      ss.GetDocRecord(d) map {
        dr =>
          val c = dr.count

          for (i <- 1L to c) {
            val j = (Random.nextDouble() * (posSoFar + i)).toInt
            if (j < k) {
              res(j) = Position.mkFromFoc(dr, i.toInt)
            }
          }

          step = c
      }

      posSoFar += step
    }
  }

  def knuthShufle[T]( a : Array[T] ) : Unit = {
    def swap[T](i : Int, j : Int) = {
      val t = a(i)
      a(i) = a(j); a(j) = t
    }

    val n = a.length
    for (i <- 0 until n reverse) {
      val j = Random.nextInt(i + 1)
      swap(i,j)
    }
  }

}

object Main extends App {

  if (args.length < 3) {
    println("Wrong number of parameters, should be >= 3")
    scala.sys.exit()
  }

  val np = args(0)
  val ksName = args(1)
  val samplesSizesList = args(2) split(",") map(_.toInt) par

  managed(new DBSession( np, ksName )) acquireAndGet {
    ss =>

      val docLimit = if (args.length >= 4) args(3).toLong else ss.lastDocId
      println(s"Doc limit is set to $docLimit")

      samplesSizesList.foreach(procRange(ss, _, docLimit))
  }

  def procRange(ss: DBSession, r : Int, dlim : Long): Unit = {

    val ksName = Option(ss.sess.getLoggedKeyspace)
    val arr = Array.ofDim[Position](r)

    ReservoirSampling.sample(ss, arr, dlim)
    ReservoirSampling.knuthShufle(arr)

    ksName foreach {
      ks =>

        val fname = s"pos.$ks.$r.txt"
        println(s"Preparing file: $fname")

        val out = Resource fromFile fname
        arr foreach { p => out write s"$p\n" }

        println(s"Done with: $fname")
    }
  }

}
