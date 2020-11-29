package org.apache.spark.ml.made

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should


class SparkTest extends AnyFlatSpec with should.Matchers{
    "spark" should "start context" in {
        var spark = SparkSession.builder
            .appName("SimpleApp")
            .master("local[4]")
            .getOrCreate()
        Thread.sleep(1000)
    }
}
