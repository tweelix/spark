/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A special case of the [[RelationalGroupedDataset]] to allow Grouping Set syntax
 *
 * @since TBD
 */
@Stable
class RelationalGroupingSetsDataset protected[sql](
    private[sql] override val df: DataFrame,
    private[sql] val groupingSets: Seq[Seq[Expression]]) extends RelationalGroupedDataset(
        df, groupingSets.flatten.distinct, RelationalGroupedDataset.GroupingSetsType
) {

  protected override final def toDF(aggExprs: Seq[Expression]): DataFrame = {
    val aggregates = if (df.sparkSession.sessionState.conf.dataFrameRetainGroupColumns) {
      groupingExprs match {
        // call `toList` because `Stream` can't serialize in scala 2.13
        case s: Stream[Expression] => s.toList ++ aggExprs
        case other => other ++ aggExprs
      }
    } else {
      aggExprs
    }

    val aliasedAgg = aggregates.map(alias)
    Dataset.ofRows(
      df.sparkSession, Aggregate(Seq(GroupingSets(groupingSets)),
        aliasedAgg, df.logicalPlan))
  }
}


private[sql] object RelationalGroupingSetsDataset {

  def apply(df: DataFrame,
            groupingSets: Seq[Seq[Expression]]): RelationalGroupedDataset = {
    new RelationalGroupingSetsDataset(df, groupingSets)
  }

}
