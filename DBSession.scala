import com.datastax.driver.core.{Cluster}
import java.lang

/**
  * User:      kaa
  * Timestamp: 02/02/16 15:41
  */
case class Position(docId : Long, archOff : Long, pos : Int ) {
  override def toString () : String = s"$docId $archOff $pos"
}

case object Position {
  def mkFromFoc(d : DocRecord, p : Int) : Position =
    Position(docId = d.id, archOff = d.arch_off, pos = p)
}

case class DocRecord(id : Long, arch_off: Long, count : Int, count_acc : Long)

class DBSession(npl:String, ks:String) {

  val npList = npl.split(DBSession.SEP)
  val sess = Cluster.builder()
    .addContactPoints(npList : _*)
    .build().connect(ks)

  println(s"Conncting to hosts: $npl")
  println(s"Key space: $ks")

  val prepDocSel = sess.prepare("select id, arch_off, count, count_acc from docs where id = ?")

  val lastDocId =
    sess.execute(s"select value from config where name = '${DBSession.LAST_DOC_OPTION}'")
      .one()
      .getString("value")
      .toLong

  val totalPosCount =
    GetDocRecord(lastDocId).getOrElse(DocRecord(0,0,0,0))
      match { case DocRecord(_,_,_,acc) => acc }

  // type JBoxedLong = java.lang.Long

  def GetDocRecord(id: Long): Option[DocRecord] = {

    val row = Option(sess.execute( prepDocSel.bind( Long.box(id) ) ).one())

    row map {r => DocRecord(
      id=r.getLong(0),
      arch_off=r.getLong(1),
      count=r.getInt(2),
      count_acc=r.getLong(3)
    ) }
  }

  def close(): Unit = {
    val cl = sess.getCluster
    sess.close()
    cl.close()
  }

}

object DBSession {
  val SEP = ','
  val LAST_DOC_OPTION = "last-doc"
}
