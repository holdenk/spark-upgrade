package fix

import scalafix.v1._
import scala.meta._

class UnionRewriteQQ extends SemanticRule("UnionRewriteQQ") {
  override val description =
    """Replacing unionAll with union only for Dataset and DataFrame"""
  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {

    def isDatasetDataFrame(
        tp: String,
        q: Term,
        a: List[Term]
    ): Boolean = {
      if (a.nonEmpty) {
        if (q.toString().indexOf("unionAll") >= 0 && tp == "DataFrame") {
          // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          // When val res: Dataset[Row]= DataFrame1.unionAll(DataFrame2) !!
          // !!!!! result type Dataset[Row] !!!!!                        !!
          // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          (q.symbol.info.get.signature.toString().indexOf("Dataset") >= 0)
            .equals(true) &&
          (a.head.symbol.info.get.signature.toString().indexOf(tp) >= 0)
            .equals(true)
        } else
          (q.symbol.info.get.signature.toString().indexOf(tp) >= 0)
            .equals(true) &&
          (a.head.symbol.info.get.signature.toString().indexOf(tp) >= 0)
            .equals(true)
      } else false
    }

    def matchOnTree(t: Tree): Patch = {
      t collect {
        case meth @ Defn.Def(a1, a2, a3, a4, a5, a6) =>
          a6.collect {
            case ta @ Term.Apply(
                  Term.Select(qual, trm @ q"""unionAll"""),
                  args
                ) =>
              if (
                isDatasetDataFrame(
                  "DataFrame",
                  qual,
                  args
                ) || isDatasetDataFrame("Dataset", qual, args)
              ) {

                Patch.replaceTree(
                  trm,
                  """union"""
                )
              } else Patch.empty
            case tasr @ Term.Apply(
                  Term.Select(qual, tnr @ q"""reduce"""),
                  args @ List(
                    Term.AnonymousFunction(
                      Term.ApplyInfix(_, op @ q"""unionAll""", _, _)
                    )
                  )
                ) =>
              if (
                qual.symbol.info.get.signature
                  .toString()
                  .indexOf("Dataset") >= 0 || qual.symbol.info.get.signature
                  .toString()
                  .indexOf("DataFrame") >= 0
              ) Patch.replaceTree(op, """union""")
              else Patch.empty
            case _ => Patch.empty
          }.asPatch
        case _ => Patch.empty
      }
    }.asPatch

    matchOnTree(doc.tree)
  }

}
