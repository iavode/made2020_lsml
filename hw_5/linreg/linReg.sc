import scala.util.Random
import scala.util.control.Breaks
import breeze.linalg.{DenseMatrix, DenseVector, sum, csvread}
import breeze.numerics.{pow, abs}

def computeMSE(yTrue: DenseVector[Double], yPredict: DenseVector[Double]): Double = {
  sum(pow(yPredict - yTrue, 2)) / yTrue.size
}

def computePredict(xInput: DenseMatrix[Double], weights: DenseVector[Double], bias: Double): DenseVector[Double] = {
  xInput * weights + bias
}

def _computeGradients(xInput: DenseMatrix[Double], yTrue: DenseVector[Double], yPredict: DenseVector[Double]) = {
  val error: DenseVector[Double] = yTrue - yPredict
  val size: Int = yTrue.size
  val weightsGrad: DenseVector[Double] = -2.0 * (xInput.t * error) / size.toDouble
  val biasGrad: Double = -2.0 * sum(error) / size.toDouble
  (weightsGrad, biasGrad)
}

def _updateWeights(weights: DenseVector[Double], weightsGrad: DenseVector[Double], learningRate: Double): DenseVector[Double] = {
  weights - learningRate * weightsGrad
}

def _updateBias(bias: Double, biasGrad: Double, learningRate: Double): Double = {
  bias - learningRate * biasGrad
}

def _norm(weights: DenseVector[Double], new_weights: DenseVector[Double]):Double = {
  sum(abs(weights - new_weights))
}

def fit(xInput: DenseMatrix[Double], yTrue: DenseVector[Double], epsilon: Double): (DenseVector[Double], Double) = {
  var iterations: Int = 0
  val learningRate: Double = 0.001
  var weights: DenseVector[Double] = DenseVector.rand(xInput.cols)
  var newWeights: DenseVector[Double] = DenseVector.zeros[Double](weights.size)
  var bias: Double = Random.nextDouble() * 0.1
  var newBias: Double = 0.0
  var yPredict: DenseVector[Double] = DenseVector.zeros[Double](yTrue.size)
  val loop = new Breaks
  loop.breakable {
    while (true) {
      yPredict = computePredict(xInput, weights, bias)
      val (weightsGrad, biasGrad) = _computeGradients(xInput, yTrue, yPredict)
      newWeights = _updateWeights(weights, weightsGrad, learningRate)
      newBias = _updateBias(bias, biasGrad, learningRate)
      if (_norm(weights, newWeights) < epsilon) {
        return (newWeights, newBias)
      }
      weights = newWeights
      bias = newBias
      if (iterations % 100 == 0) {
        println(s"$iterations) Error: ${computeMSE(yTrue, yPredict)}")
      }
      iterations += 1
    }
  }
  (weights, bias)
}


val samples: Int = 100
val features: Int = 4
val yTrue: DenseVector[Double] = DenseVector.fill(samples)(1 + Random.nextDouble())
val xInput: DenseMatrix[Double] = DenseMatrix.rand(samples, features)
val (weights, bias) = fit(xInput, yTrue, epsilon = 1e-5)
val row: Int = xInput.cols - 1
xInput(::, row)







