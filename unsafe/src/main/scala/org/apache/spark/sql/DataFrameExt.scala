package org.apache.spark.sql

import org.apache.spark.sql.BebeFunctions.{assertNotNull, knownNotNull, knownNullable}
import org.apache.spark.sql.functions.{col, struct, transform}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object DataFrameExt {

  /**
   * Aligns the nullability of the columns in the original schema to match the target schema.
   * Will still include columns that are not present in the original schema, but will not assert nullability on them.
   * Will use target schema data types if they differ from the original schema.
   */
  private def alignSchemaNullability(
      originalSchema: StructType,
      targetSchema: StructType,
      alignNotNullToNullable: Boolean
  ): Seq[Column] = {
    def childFieldToCol(
        orgChildField: Option[StructField],
        targetChildField: StructField,
        parentCol: Column,
        firstLevel: Boolean = false
    ): Column = {
      orgChildField match {
        case Some(oField) =>
          val fieldCol = (oField.dataType, targetChildField.dataType) match {
            case (orgStruct: StructType, tarStruct: StructType) =>
              val fields = tarStruct.map { tarField =>
                childFieldToCol(
                  orgStruct.find(_.name == tarField.name),
                  tarField,
                  tarField.dataType match {
                    case StructType(_) | ArrayType(_: StructType, _) => parentCol(tarField.name)
                    case _                                           => parentCol
                  }
                ).as(tarField.name)
              }

              struct(fields: _*)
            case (ArrayType(oInnerType, _), ArrayType(tInnerType, _)) =>
              transform(
                parentCol,
                childCol => childFieldToCol(Some(oField.copy(dataType = oInnerType)), targetChildField.copy(dataType = tInnerType), childCol)
              )
            case _ if firstLevel  => parentCol
            case _ if !firstLevel => parentCol(targetChildField.name)
          }

          val alignedField = (targetChildField.nullable, oField.nullable, alignNotNullToNullable) match {
            case (false, true, _)     => assertNotNull(fieldCol)
            case (true, false, true)  => knownNullable(fieldCol)
            case (true, false, false) => fieldCol
            case (false, false, _)    => knownNotNull(fieldCol)
            case (true, true, _)      => knownNullable(fieldCol)
          }

          alignedField.as(targetChildField.name)
        case None if firstLevel  => parentCol
        case None if !firstLevel => parentCol(targetChildField.name)
      }
    }

    targetSchema.map(tField => childFieldToCol(originalSchema.find(_.name == tField.name), tField, col(tField.name), firstLevel = true))
  }

  implicit class DataFrameMethods(df: DataFrame) {

    /**
     * Converts the DataFrame to the specified target schema, aligning nullability if required.
     * This is similar to `df.to(targetSchema)` but allows for nullability alignment by asserting nullability
     * for each column where the original nullability does not match the target nullability.
     *
     * @param targetSchema           The target schema to convert to.
     * @param alignNotNullToNullable by default if original column is not nullable and target column is nullable, it will not make it nullable.
     *                               If set to true, it will align target Dataframe column to nullable.
     * @return A new DataFrame with the specified schema.
     */
    def toSchemaWithNullabilityAligned(targetSchema: StructType, alignNotNullToNullable: Boolean = false): DataFrame = {
      val columns = alignSchemaNullability(df.schema, targetSchema, alignNotNullToNullable)
      df.select(columns: _*)
    }
  }
}
