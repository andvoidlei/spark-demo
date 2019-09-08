package examples.als.util




import com.github.fommil.netlib.BLAS.{getInstance => blas}
import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.generic.Growable


class BlasSim(val numItems: Int, val vectorSize: Int, val itemVectors: Array[Float], val itemIndex: Map[java.lang.String, Int])extends Serializable{

  val itemList = {
    val (wl, _) = itemIndex.toSeq.sortBy(_._2).unzip
    wl.toArray
  }

  val wordVecNorms: Array[Double] = {
    val wordVecNorms = new Array[Double](numItems)
    var i = 0
    while (i < numItems) {
      val vec = itemVectors.slice(i * vectorSize, i * vectorSize + vectorSize)
      wordVecNorms(i) = blas.snrm2(vectorSize, vec, 1)
      i += 1
    }
    wordVecNorms
  }

  def getCosinSimilarity(vector:Vector[Float], num: Int, wordOpt: Option[String]): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")
    val fVector = vector.toArray.map(_.toFloat)
    val cosineVec = Array.fill[Float](numItems)(0)
    val alpha: Float = 1
    val beta: Float = 0
    // 归一化输入向量
    val vecNorm = blas.snrm2(vectorSize, fVector, 1)
    if (vecNorm != 0.0f) {
      blas.sscal(vectorSize, 1 / vecNorm, fVector, 0, 1)
    }
    blas.sgemv("T", vectorSize, numItems, alpha, itemVectors, vectorSize, fVector, 1, beta, cosineVec, 1)
    val cosVec = cosineVec.map(_.toDouble)
    var ind = 0
    while (ind < numItems) {
      val norm = wordVecNorms(ind)
      if (norm == 0.0) {
        cosVec(ind) = 0.0
      } else {
        cosVec(ind) /= norm
      }
      ind += 1
    }
    val pq = new BoundedPriorityQueue[(String, Double)](num + 1)(Ordering.by(_._2))
    for (i <- cosVec.indices) {
      pq += Tuple2(itemList(i), cosVec(i))
    }

    val scored = pq.toSeq.sortBy(-_._2)

    val filtered = wordOpt match {
      case Some(w) => scored.filter(tup => w != tup._1)
      case None => scored
    }
    filtered.take(num).toArray
  }
}

class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach {
      this += _
    }
    this
  }

  override def +=(elem: A): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {
      maybeReplaceLowest(elem)
    }
    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear() {
    underlying.clear()
  }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}
