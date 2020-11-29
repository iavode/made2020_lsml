package org.apache.spark.ml.made

import breeze.linalg.{DenseVector, sum}
import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import com.google.common.io.Files
import org.apache.spark.ml.Pipeline

import scala.util.Random

class LinearRegressionTest extends AnyFlatSpec with should.Matchers with WithSpark {

    val delta = 1e-4
    lazy val randomState = LinearRegressionTest._randomState
    lazy val data = LinearRegressionTest._data
    lazy val bigData = LinearRegressionTest._bigData
    lazy val testBigData = LinearRegressionTest._testBigData
    lazy val weights = LinearRegressionTest._weights
    lazy val weightsBigData = LinearRegressionTest._weightsBigData
    lazy val bias = LinearRegressionTest._bias

    def validateModel(model: ml.Transformer, data: DataFrame = data, delta: Double = delta): Assertion = {

        val result = model.transform(data).collect()
        val yPredict: Array[Double] = result.map(_.getAs[Double](2))
        val yTrue: Array[Double] = result.map(_.getAs[Double](1))

        yTrue(0) should be(yPredict(0) +- delta)
        yTrue(1) should be(yPredict(1) +- delta)
        yTrue(2) should be(yPredict(2) +- delta)
    }


    "model" should "compute predict" in {
        val model: LinearRegressionModel = new LinearRegressionModel(weights, bias)
            .setFeaturesCols("features")
            .setLabelCol("target")

        validateModel(model)
    }

    "estimator" should "correct compute model params in relation to dataset" in {
        // test work 1.7 min on Intel® Core™ i5-9600K CPU @ 3.70GHz × 6
        val estimator = new LinearRegression(C = 3e-3, tol=0.00005, maxIter=1500, randomState=randomState)
            .setFeaturesCols("features")
            .setLabelCol("target")
        val model = estimator.fit(bigData)
        val modeWeights = model.weights
        val modeBias = model.bias
        weightsBigData(0) should be(modeWeights(0) +- delta)
        weightsBigData(1) should be(modeWeights(1) +- delta)
        weightsBigData(2) should be(modeWeights(2) +- delta)
        bias should be(modeBias +- delta)
    }

    "estimator" should "create models, which calculate predict" in {
        // test work 1.7 min on Intel® Core™ i5-9600K CPU @ 3.70GHz × 6
        val estimator = new LinearRegression(C = 3e-3, tol=0.00005, maxIter=1500, randomState=randomState)
            .setFeaturesCols("features")
            .setLabelCol("target")
        val model = estimator.fit(bigData)
        validateModel(model, testBigData, 0.02)
    }

    "estimator" should "work after re-read" in {
        // test work 1.7 min on Intel® Core™ i5-9600K CPU @ 3.70GHz × 6
        val pipeline = new Pipeline().setStages(Array(
            new LinearRegression(C = 3e-3, tol=0.00005, maxIter=1500, randomState=randomState)
                .setFeaturesCols("features")
                .setLabelCol("target")
        ))
        val tmpFolder = Files.createTempDir()
        pipeline.write.overwrite().save(tmpFolder.getAbsolutePath)

        val model = Pipeline.load(tmpFolder.getAbsolutePath).fit(bigData).stages(0).asInstanceOf[LinearRegressionModel]

        val modeWeights = model.weights
        val modeBias = model.bias
        weightsBigData(0) should be(modeWeights(0) +- delta)
        weightsBigData(1) should be(modeWeights(1) +- delta)
        weightsBigData(2) should be(modeWeights(2) +- delta)
        bias should be(modeBias +- delta)
    }

}

object LinearRegressionTest extends WithSpark {
    lazy val _randomState: Long = 42
    Random.setSeed(_randomState)
    import sqlc.implicits._
    // true model weights
    lazy val _weights = Vectors.dense(0.2, 0.3).toDense
    lazy val _bias = 0.1
    // synthetic data features columns
    lazy val _features = Seq(
        Vectors.dense(1.0, 2.0).toDense,
        Vectors.dense(3.0, 4.0).toDense,
        Vectors.dense(-4.0, -5.0).toDense
    )
    lazy val _data: DataFrame = {
        _features.map(
            feature => {
                Tuple2(feature, feature.asBreeze.t * _weights.asBreeze + _bias)
            }
        ).toDF("features", "target")
    }
    // synthetic big data features columns
    lazy val N = 100000
    lazy val _weightsBigData = Vectors.dense(0.2, 0.3, 0.4).toDense
    lazy val _bigFeatures = (0 until N).map(i => {
        val feature: DenseVector[Double] = {
            DenseVector.fill(size = _weightsBigData.size)(Random.nextGaussian() * 5)
        }
        Vectors.dense(feature.toArray).toDense
    })

    lazy val _bigData: DataFrame = _bigFeatures.map(feature => {
        Tuple2(feature, sum(feature.asBreeze * _weightsBigData.asBreeze) + _bias + Random.nextGaussian() * 0.01)
    }).toDF("features", "target")

    lazy val _testBigFeatures = (0 until 3).map(i => {
        val feature: DenseVector[Double] = {
            DenseVector.fill(size = _weightsBigData.size)(Random.nextGaussian() * 5)
        }
        Vectors.dense(feature.toArray).toDense
    })
    lazy val _testBigData = _testBigFeatures.map(feature => {
        Tuple2(feature, sum(feature.asBreeze * _weightsBigData.asBreeze) + _bias + Random.nextGaussian() * 0.01)
    }).toDF("features", "target")
}
