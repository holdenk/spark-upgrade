package fix
import scalafix.v1._
import scala.meta._

class GroupByKeyRewrite extends SemanticRule("GroupByKeyRewrite") {
  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {
    val grpByKey = "groupByKey"
    val funcToDS = "toDS"
    val agrFunCount = "count"
    val oprCol = "withColumnRenamed"
    val colNameOld = "value"
    val colNameNew = "key"

    def matchOnTree(t: Tree): Patch = {
      t.collect {
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(fName)
                          ),
                          _
                        ),
                        _ @Term.Name(grpByKeyName)
                      ),
                      _
                    ),
                    _ @Term.Name(oprName)
                  ),
                  _
                ),
                _ @Term.Name(oprColumnName)
              ),
              List(oldColName @ Lit.String(valueOld), _)
            )
            if grpByKey
              .equals(grpByKeyName) && funcToDS.equals(fName) && agrFunCount
              .equals(oprName) && oprCol.equals(oprColumnName) && colNameOld
              .equals(valueOld) =>
          Patch.replaceTree(oldColName, "\"".concat(colNameNew).concat("\""))
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        _ @Term.Name(grpByKeyName)
                      ),
                      _
                    ),
                    _ @Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(selectName)
              ),
              List(
                Term.Interpolate(
                  _ @Term.Name(cName),
                  List(colOld @ Lit.String(colOldName)),
                  _
                ),
                _
              )
            )
            if grpByKey
              .equals(grpByKeyName) && funcToDS.equals(toDSName) && agrFunCount
              .equals(countName) && "select".equals(selectName) && "$"
              .equals(cName) && "value".equals(colOldName) =>
          Patch.replaceTree(colOld, colNameNew)
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        _ @Term.Name(groupByKeyName)
                      ),
                      _
                    ),
                    _ @Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(selectName)
              ),
              List(
                Term.Apply(
                  _ @Term.Name(colName),
                  List(oldNameColumn @ Lit.String(valueName))
                ),
                _
              )
            )
            if funcToDS.equals(toDSName) && grpByKey.equals(
              groupByKeyName
            ) && "count".equals(
              countName
            ) && "select".equals(selectName) && "col"
              .equals(colName) && colNameOld.equals(valueName) =>
          Patch.replaceTree(oldNameColumn, "\"".concat(colNameNew).concat("\""))
        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        _ @Term.Name(groupByKeyName)
                      ),
                      _
                    ),
                    _ @Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(selectName)
              ),
              List(
                oldNameColumn @ Lit.Symbol(valueName),
                _
              )
            )
            if funcToDS.equals(toDSName) && grpByKey.equals(
              groupByKeyName
            ) && "count".equals(
              countName
            ) && "select"
              .equals(selectName) && "'value".equals(valueName.toString()) =>
          Patch.replaceTree(oldNameColumn, "'".concat(colNameNew))

        case Term.Apply(
              Term.Select(
                Term.Apply(
                  Term.Select(
                    Term.Apply(
                      Term.Select(
                        Term.Apply(
                          Term.Select(
                            _,
                            _ @Term.Name(toDSName)
                          ),
                          _
                        ),
                        _ @Term.Name(groupByKeyName)
                      ),
                      _
                    ),
                    _ @Term.Name(countName)
                  ),
                  _
                ),
                _ @Term.Name(withColumnName)
              ),
              List(
                _,
                Term.Apply(
                  _,
                  List(
                    Term.Apply(
                      _ @Term.Name(colName),
                      List(oldNameColumn @ Lit.String(valueName))
                    )
                  )
                )
              )
            )
            if funcToDS.equals(toDSName) && grpByKey.equals(
              groupByKeyName
            ) && "count".equals(
              countName
            ) && "withColumn".equals(withColumnName) && "col".equals(
              colName
            ) && "value".equals(valueName) =>
          Patch.replaceTree(oldNameColumn, "\"".concat(colNameNew).concat("\""))
      }.asPatch
    }

    matchOnTree(doc.tree)
  }

}
