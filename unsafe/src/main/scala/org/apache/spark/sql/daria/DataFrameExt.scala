package org.apache.spark.sql.daria
import org.apache.spark.sql.daria.functions.{assertNotNull, knownNotNull, knownNullable}
import org.apache.spark.sql.functions.{col, lit, struct, transform, when}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameExt {

  /**
   * Aligns the nullability of the columns in the original schema to match the target schema.
   * Will ignore columns that are not present in the original schema.
   * Will not cast from original data type to target data type.
   */
  private def alignSchemaNullability(originalSchema: StructType, targetSchema: StructType, alignNotNullToNullable: Boolean): Seq[Column] = {
    def childFieldToCol(orgChildField: StructField, targetChildField: StructField, parentCol: Column, firstLevel: Boolean = false): Column = {
      val fieldCol = (orgChildField.dataType, targetChildField.dataType) match {
        case (orgStruct: StructType, tarStruct: StructType) =>
          val fields = orgStruct.map { orgField =>
            tarStruct.find(_.name == orgField.name) match {
              case Some(tarField) => childFieldToCol(
                orgField,
                tarField,
                orgField.dataType match {
                  case StructType(_) | ArrayType(_: StructType, _) => parentCol(orgField.name)
                  case _                                           => parentCol
                }
              ).as(orgField.name)
              case None => parentCol(orgField.name).as(orgField.name)
            }
          }

          struct(fields: _*)
        case (ArrayType(oInnerType, _), ArrayType(tInnerType, _)) =>
          transform(
            parentCol,
            childCol => childFieldToCol(orgChildField.copy(dataType = oInnerType), targetChildField.copy(dataType = tInnerType), childCol)
          )
        case _ if firstLevel  => parentCol
        case _ if !firstLevel => parentCol(orgChildField.name)
      }

      val alignedField = (targetChildField.nullable, orgChildField.nullable, alignNotNullToNullable) match {
        case (false, true, _)     => assertNotNull(fieldCol)
        case (true, false, true)  => knownNullable(fieldCol)
        case (true, false, false) => fieldCol
        case (false, false, _)    => knownNotNull(fieldCol)
        case (true, true, _)      => knownNullable(fieldCol)
      }

      alignedField.as(orgChildField.name)
    }

    originalSchema.map { oField =>
      val f = targetSchema.find(_.name == oField.name) match {
        case Some(tField) => childFieldToCol(oField, tField, col(oField.name), firstLevel = true)
        case None         => col(oField.name)
      }
      f.as(oField.name)
    }
  }

  implicit class DataFrameMethods(df: DataFrame) {

    /**
     * Converts the DataFrame to the specified target schema, aligning nullability if required.
     * This is essentially the same as `df.to(targetSchema)` but allows for nullability alignment by asserting nullability
     * for each column where the original nullability does not match the target nullability.
     *
     * @param targetSchema The target schema to convert to.
     * @param alignToNullable by default if original column is not nullable and target column is nullable, it will not make it nullable.
     *                        If set to true, it will align target Dataframe column to nullable.
     * @return A new DataFrame with the specified schema.
     */
    def toSchemaWithNullabilityAligned(targetSchema: StructType, alignNotNullToNullable: Boolean = false): DataFrame = {
      val columns = alignSchemaNullability(df.schema, targetSchema, alignNotNullToNullable)
      df.select(columns: _*).show(false)
      df.select(columns: _*).printSchema()
      df.select(columns: _*).to(targetSchema)
    }
  }
}
