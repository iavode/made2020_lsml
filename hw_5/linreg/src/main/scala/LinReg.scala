import java.io.{File, PrintWriter}
import LinReg.computeMSE
import breeze.linalg.{DenseMatrix, DenseVector, csvread, csvwrite, sum}
import breeze.numerics.{abs, pow}
import breeze.stats.mean

import scala.util.Random
import scala.util.control.Breaks

class LinearRegression(learningRate: Double, epsilon: Double, verbose_n: Int = 5000) {

  private var weights: DenseVector[Double] = DenseVector.zeros[Double](2)
  private var bias: Double = Random.nextDouble() * 0.01

  private def _computeGradients(xInput: DenseMatrix[Double], yTrue: DenseVector[Double], yPredict: DenseVector[Double]): Tuple2(DenseVector[Double], Double) = {
    val error: DenseVector[Double] = yTrue - yPredict
    val size: Int = yTrue.size
    val weightsGrad: DenseVector[Double] = -2.0 * (xInput.t * error) / size.toDouble
    val biasGrad: Double = -2.0 * mean(error)
    Tuple2(weightsGrad, biasGrad)
  }

  private def _updateWeights(weightsGrad: DenseVector[Double]): DenseVector[Double] = {
    weights - learningRate * weightsGrad
  }

  private def _updateBias(biasGrad: Double): Double = {
    bias - learningRate * biasGrad
  }

  private def _norm(weights: DenseVector[Double], new_weights: DenseVector[Double]) = {
    sum(abs(weights - new_weights))
  }

  def predict(xInput: DenseMatrix[Double]): DenseVector[Double] = {
    xInput * weights + bias
  }

  def fit(xInput: DenseMatrix[Double], yTrue: DenseVector[Double]): Unit = {
    var iterations: Integer = 0
    weights = DenseVector.rand[Double](xInput.cols)
    var newWeights: DenseVector[Double] = DenseVector.zeros[Double](weights.size)
    var newBias: Double = 0.0
    var yPredict: DenseVector[Double] = DenseVector.zeros[Double](yTrue.size)
    val loop = new Breaks
    loop.breakable {
      while (true) {
        yPredict = predict(xInput)
        val (weightsGrad, biasGrad) = _computeGradients(xInput, yTrue, yPredict)
        newWeights = _updateWeights(weightsGrad)
        newBias = _updateBias(biasGrad)
        if (_norm(weights, newWeights) < epsilon) loop.break()
        if (iterations % verbose_n == 0) {
          println(s"$iterations) Error: ${computeMSE(yTrue, yPredict)}")
        }
        weights = newWeights
        bias = newBias
        iterations += 1
      }
    }
    println(s"$iterations) Error: ${computeMSE(yTrue, yPredict)}")
  }
}

object LinReg extends App {
  def splitTrainTest(data: DenseMatrix[Double], trainSize: Double): (DenseMatrix[Double], DenseMatrix[Double], DenseVector[Double], DenseVector[Double]) = {
    val size: Int = (trainSize * data.rows).toInt
    val dataTrain: DenseMatrix[Double] = data(0 until size, ::)
    val dataTest: DenseMatrix[Double] = data(size to -1, ::)
    val dataTrainX: DenseMatrix[Double] = dataTrain(::, 0 to -2)
    val dataTrainY: DenseVector[Double] = dataTrain(::, -1)
    val dataTestX: DenseMatrix[Double] = dataTest(::, 0 to -2)
    val dataTestY: DenseVector[Double] = dataTest(::, -1)
    (dataTrainX, dataTestX, dataTrainY, dataTestY)
  }

  def computeMSE(yTrue: DenseVector[Double], yPredict: DenseVector[Double]): Double = {
    mean(pow(yPredict - yTrue, 2))
  }

  def writeToCsvFile(fileName: String, yPredict: DenseVector[Double], dataTestY: DenseVector[Double]) : Unit ={
    val outputFile: File = new File(fileName)
    val output: DenseMatrix[Double] = DenseMatrix.zeros[Double](yPredict.length, 2)
    for(i <- 0 until yPredict.length){
      output(i, 0) = dataTestY(i)
      output(i, 1) = yPredict(i)
    }
    csvwrite(outputFile, output)
  }

def shuffle(dataTrainX: DenseMatrix[Double], dataTrainY: DenseVector[Double],
            rows: Int, cvRowSize: Int): (DenseMatrix[Double], DenseVector[Double], DenseMatrix[Double], DenseVector[Double])= {
    val cvDataTrainX: DenseMatrix[Double] = DenseMatrix.zeros[Double](cvRowSize, dataTrainX.cols)
    val cvDataTrainY: DenseVector[Double] = DenseVector.zeros[Double](cvRowSize)
    val cvDataTestX: DenseMatrix[Double] = DenseMatrix.zeros[Double](rows - cvRowSize, dataTrainX.cols)
    val cvDataTestY: DenseVector[Double] = DenseVector.zeros[Double](rows - cvRowSize)
    val index = Random.shuffle(for (i <- 0 until rows) yield i)
    for(i <- 0 until rows) {
      if (i < cvRowSize) {
        cvDataTrainX(i, ::) := dataTrainX(index(i), ::)
        cvDataTrainY(i) = dataTrainY(index(i))
      }
      else {
        cvDataTestX(i - cvRowSize, ::) := dataTrainX(index(i), ::)
        cvDataTestY(i - cvRowSize) = dataTrainY(index(i))
      }
    }
    (cvDataTrainX, cvDataTrainY, cvDataTestX, cvDataTestY)
  }

  def crossValIteration(model: LinearRegression, dataTrainX: DenseMatrix[Double], dataTrainY: DenseVector [Double],
                         dataTestX: DenseMatrix[Double], dataTestY: DenseVector[Double]) : Double = {
    model.fit(dataTrainX, dataTrainY)
    computeMSE(model.predict(dataTestX), dataTestY)
  }

  def crossValidation(model: LinearRegression, dataTrainX: DenseMatrix[Double],
                      dataTrainY: DenseVector[Double], cvSize: Int = 5): DenseVector[Double] = {
    val rows: Int = dataTrainX.rows
    val cvRowSize = (rows * 0.8).toInt
    val cvResult: DenseVector[Double] = DenseVector.zeros[Double](cvSize)
    for(i <- 0 until cvSize) {
      println(s"Cross-validation iteration - ${i + 1}")
      val (cvDataTrainX, cvDataTrainY, cvDataTestX, cvDataTestY) = shuffle(dataTrainX, dataTrainY, rows, cvRowSize)
      cvResult(i) = crossValIteration(model, cvDataTrainX, cvDataTrainY, cvDataTestX, cvDataTestY)
    }
    cvResult
  }

  // get data
  val pathToFile: String = "/std_dataset.csv"
  val data: DenseMatrix[Double] = csvread(new File(getClass.getResource(pathToFile).getPath), ',')
  // create model
  val model: LinearRegression = new LinearRegression(learningRate = 0.001, epsilon = 0.0001)
  // data split
  val (dataTrainX, dataTestX, dataTrainY, dataTestY) = splitTrainTest(data, trainSize = 0.8)
  val cvResults: DenseVector[Double] = crossValidation(model, dataTrainX, dataTrainY)
  val meanCV: Double = mean(cvResults)
  println(s"Cross-validation values: $cvResults")
  // fit on train data
  model.fit(dataTrainX, dataTrainY)
  // predict
  val yPredict: DenseVector[Double] = model.predict(dataTestX)
  // output true values, predicted and MSE
  println(s"True value: ${dataTestY(0 until 5)}")
  println(s"Predict value ${yPredict(0 until 5)}")
  println(s"Validation MSE: ${computeMSE(dataTestY, yPredict)}")
  writeToCsvFile("output.csv", yPredict, dataTestY)
  // output cross-validation results
  val fOut = new PrintWriter(new File("cv_results.txt"))
  fOut.write("Cross-validation score:\n")
  cvResults.map(result => fOut.write(result.toString + " "))
  fOut.write("\n")
  fOut.write(s"Mean cross-validation value: $meanCV\n")
  fOut.close()
}
