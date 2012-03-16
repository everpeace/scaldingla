import cascading.pipe.Pipe
import com.twitter.scalding._

/**
 * Matrix Multiplication in Scalding.
 *
 * @author Shingo Omura (everpeace _at_ gmail _dot_ com)
 */
class MatrixMult(args: Args) extends Job(args) {
    val As = TextLine("data/A.txt")
    val Bs = TextLine("data/B.txt")
    val Cs = TextLine("data/C.txt")

    // A is I x J matrix: each row of A is (i,j,a_{i,j})
    val (rowsA,colsA,_A) = decomposeMatrix(As)
    val A = _A.rename(('row,'column,'val)->('ia,'ja,'a_ij))

    // B is J x K matrix: each row of B is (j,k,b_{j,k})
    val (rowsB,colsB,_B) = decomposeMatrix(Bs)
    val B = _B.rename(('row,'column,'val)->('jb,'kb,'b_jk))

    //join with 'ja == 'jb
    // and calculate c_{i,k}= a_{i,j} * b_{j,k}
    val C = A.joinWithSmaller( 'ja -> 'jb, B)
      .map(('a_ij, 'b_jk) -> 'ab) { ab : (Double, Double) => ab._1 * ab._2 }
      .project('ia, 'kb, 'ab)
      .groupBy('ia,'kb) { _.sum('ab) }
      .rename(('ia,'kb,'ab) -> ('ic,'kc,'cik))

     composeMatrix(C.rename(('ic,'kc,'cik)->('row,'column,'val)))
       .write(Cs)

    // make matrix data
    // [0 0 a00]
    // ...
    // [i j aij]
    // ...
    def decomposeMatrix(s:TextLine)
    = {
      // decompose [ai0 ai1 ...] to [i 0 ai0]
      //                            [i 1 ai1]
      //                            ...
      val mat = s.read.rename('num -> 'row)
        .flatMap('line -> ('column,'val)){ s:String => {
          val cols = s.split("\\s")
          val index = 0 until cols.length
          index.zip(cols)
         }}.project(('row,'column,'val))

      // calculate the number of rows
      val rows = mat.project('row).groupAll{_.max('row)}
        .map('row->'row){rowMax:Int => rowMax+1}.rename('row->'rows)
      // calculate the number of columns
      val cols = mat.project('column).groupAll{_.max('column)}
        .map('column->'column){colMax:Int => colMax+1}.rename('column->'cols)

      (rows,cols,mat)
    }

    // format matrix data for output
    def composeMatrix(p:Pipe)
    = p.groupBy('row){_.sortBy('column).toList[String]('val -> '_rows)}
        .map('_rows -> 'rows){row:List[Double] => row.mkString(" ")}
        .groupAll{_.sortBy('row)}.project('rows)

}
