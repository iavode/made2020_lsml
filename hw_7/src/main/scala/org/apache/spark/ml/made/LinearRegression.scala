package org.apache.spark.ml.made

import breeze.linalg.DenseVector
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWritable, MLWriter, SchemaUtils}
import org.apache.spark.{ml, mllib}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.apache.spark.sql.types.StructType

import scala.util.Random
import scala.util.control.Breaks

trait LinearRegressionParams extends HasFeaturesCol with HasLabelCol with HasOutputCol {
    def setFeaturesCols(value: String): this.type = set(featuresCol, value)
    def setLabelCol(value: String): this.type = set(labelCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)

    protected def validateAndTransformSchema(schema: StructType): StructType = {
        SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT())
        SchemaUtils.checkNumericType(schema, $(labelCol))
        if (schema.fieldNames.contains($(outputCol))) {
            SchemaUtils.checkColumnType(schema, $(outputCol), new VectorUDT)
            schema
        }
        else {
            SchemaUtils.appendColumn(schema, schema($(labelCol)).copy(name=$(outputCol)))
        }
    }
}

class LinearRegression(override val uid: String, C: Double, tol: Double, maxIter: Int, randomState: Long)
    extends Estimator[LinearRegressionModel] with LinearRegressionParams with MLWritable {

    def this(C: Double, tol: Double, maxIter: Int, randomState: Long) = {
        this(Identifiable.randomUID("LinearRegression"), C, tol, maxIter, randomState)
    }
    private[made] def this(uid: String) = this(uid, 0.01, 0.1, 100,42)

    private case class Params(C: Double, tol: Double, maxIter: Double, randomState: Double)

    private var weights = Vectors.zeros(0)
    Random.setSeed(randomState)
    private var bias: Double = 0
    private var numCols: Int = 0

    private def computeWeightsAndBias(dataset: Dataset[_]): Unit = {

        implicit val encoder: Encoder[Vector] = ExpressionEncoder()
        var weightsBreeze: DenseVector[Double] = weights.asBreeze.toDenseVector
        val loop = new Breaks
        var iter = 0
        loop.breakable{
            while (true) {
                val gradients = dataset.rdd.mapPartitions(data => {
                    val summarizer = new MultivariateOnlineSummarizer()
                    data.foreach{ case Row(feature: Vector, y: Double) => {
                        val featureBreeze: DenseVector[Double] = feature.asBreeze.toDenseVector
                        val error: Double = (featureBreeze.t * weightsBreeze + bias) - y
                        val gradientsWeights: DenseVector[Double] = 2.0 * error * featureBreeze
                        val gradientsBias: Double = 2.0 * error
                        val result = DenseVector.vertcat(gradientsWeights, DenseVector(gradientsBias, error * error))
                        summarizer.add(mllib.linalg.Vectors.fromBreeze(result))
                    }}
                    Iterator(summarizer)
                }).reduce(_ merge _)
                val gradientsBreeze = gradients.mean.asBreeze
                val gradientWeights = gradientsBreeze(0 until numCols).toDenseVector
                val gradientBias = gradientsBreeze(numCols)
                val error_2 = gradientsBreeze(-1)
                val newWeights: DenseVector[Double] = (weightsBreeze - C * gradientWeights)
                val newBias = bias - gradientBias * C

                if (error_2 < tol || iter > maxIter) {
                    weights = Vectors.dense(newWeights.toArray)
                    bias = newBias
                    loop.break()
                }
                iter += 1
                weightsBreeze := newWeights
                bias = newBias
            }
        }
    }

    override def fit(dataset: Dataset[_]): LinearRegressionModel = {
        // prepare weights, random initialize
        implicit val encoder: Encoder[Vector] = ExpressionEncoder()
        val tmp = dataset.select($(featuresCol)).first()
        numCols = tmp(0).asInstanceOf[ml.linalg.DenseVector].size
        val _weights: Array[Double] = DenseVector.fill(size = numCols)(0.01).toArray
        weights = Vectors.dense(_weights)
        // main optimization function, implement GD
        computeWeightsAndBias(dataset)
        copyValues(new LinearRegressionModel(weights, bias)).setParent(this)
    }

    override def copy(extra: ParamMap): Estimator[LinearRegressionModel] = copyValues(new LinearRegression(C, tol, maxIter, randomState))

    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

    override def write: MLWriter = new DefaultParamsWriter(this) {
        override def saveImpl(path: String): Unit = {
            super.saveImpl(path)
            val data = Params(C, tol, maxIter.toDouble, randomState.toDouble)
            sqlContext.createDataFrame(Seq(data)).write.parquet(path + "/params")
        }
    }
}

object LinearRegression extends MLReadable[LinearRegression] {
    override def read: MLReader[LinearRegression] = new MLReader[LinearRegression] {
        override def load(path: String): LinearRegression = {
            val metadata = DefaultParamsReader.loadMetadata(path, sc)
            val params = sqlContext.read.parquet(path + "/params")
            implicit val encoder: Encoder[Double] = ExpressionEncoder()
            val Row(c: Double, tol: Double, maxIter: Double, randomState: Double) = params
                .select("C", "tol","maxIter", "randomState").head()
            val model = new LinearRegression(c, tol, maxIter.toInt, randomState.toLong)
            metadata.getAndSetParams(model)
            model
        }
    }
}

class LinearRegressionModel private[made](override val uid: String, val weights: Vector, val bias: Double)
    extends Model[LinearRegressionModel] with LinearRegressionParams {

    private[made] def this(weights: Vector, bias: Double) = this(
        Identifiable.randomUID("LinearRegressionModel"), weights, bias
    )

    override def copy(extra: ParamMap): LinearRegressionModel = copyValues(new LinearRegressionModel(weights, bias))

    override def transform(dataset: Dataset[_]): DataFrame = {
        val weightsBreeze = weights.asBreeze
        val transformUdf = dataset.sqlContext.udf.register(
            uid + "_transform",
            (features: Vector) => {
                val predict = features.asBreeze.t * weightsBreeze + bias
                predict
            }
        )
        dataset.withColumn($(outputCol), transformUdf(dataset($(featuresCol))))
    }
    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}