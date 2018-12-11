package com.github.def1ne

import breeze.io.CSVReader
import breeze.linalg.{CSCMatrix, DenseMatrix, _}

import scala.annotation.tailrec
import scala.io.Source

object BreezeBoot extends App {

  def matrixData(): IndexedSeq[IndexedSeq[String]] = {
    val mapFileURLString: String = "http://s3-ap-southeast-1.amazonaws.com/geeks.redmart.com/coding-problems/map.txt"
    val mapFileSource = Source.fromURL(mapFileURLString)
    CSVReader.read(input = mapFileSource.reader(), separator = ' ', skipLines = 0)
  }

  def testMatrixData(): IndexedSeq[IndexedSeq[String]] = {
    val testData: String = "4 4\n4 8 7 3\n2 5 9 3\n6 3 2 5\n4 4 1 6"
    CSVReader.parse(input = testData, separator = ' ', skipLines = 0)
  }

  /**
    * Transform original matrix row and column to adjacency vector position.
    *
    * @param originalMatrixData original matrix with point heights in it.
    * @param row                original matrix row.
    * @param col                original matrix column.
    * @return adjacency vector position.
    */
  def rowAndColToAdjacencyVectorIndex(originalMatrixData: Matrix[Int], row: Int, col: Int): Int =
    originalMatrixData.rows * row + col

  /**
    * Transform adjacency vector position to the original matrix row and column.
    *
    * @param originalMatrixData original matrix with point heights in it.
    * @param index              adjacency vector position.
    * @return (original matrix row, original matrix column).
    */
  def adjacencyVectorIndexToRowAndCol(originalMatrixData: Matrix[Int], index: Int): (Int, Int) = {
    val row = index / originalMatrixData.rows
    val col = index - originalMatrixData.rows * row
    (row, col)
  }

  /**
    * Build adjacency matrix from original matrix.
    *
    * @param originalMatrixData original matrix with point heights in it.
    * @return adjacency matrix.
    */
  def buildAdjacencyMatrix(originalMatrixData: Matrix[Int]): CSCMatrix[Int] = {
    //build adjacency matrix points vector
    val adjacencyVector: Iterable[(Int, Int)] = 0 until originalMatrixData.rows flatMap {
      row =>
        0 until originalMatrixData.cols map {
          col => (row, col)
        }
    }
    //build adjacency matrix
    val adjacencyMatrixBuilder = new CSCMatrix.Builder[Int](rows = adjacencyVector.size, cols = adjacencyVector.size)
    adjacencyVector.foreach({
      case (row, col) =>
        val adjacencyRow = rowAndColToAdjacencyVectorIndex(originalMatrixData, row, col)
        println(s"adjacency matrix build, row $adjacencyRow/${adjacencyVector.size}")
        val original = originalMatrixData(row, col)

        if (col > 0) { //if we have a candidate from the top
          val candidate = originalMatrixData(row, col - 1)
          if (original > candidate) {
            val adjacencyCol = rowAndColToAdjacencyVectorIndex(originalMatrixData, row, col - 1)
            adjacencyMatrixBuilder.add(adjacencyRow, adjacencyCol, 1)
          }
        }

        if (col < originalMatrixData.rows - 1) { //if we have a candidate from the bottom
          val candidate = originalMatrixData(row, col + 1)
          if (original > candidate) {
            val adjacencyCol = rowAndColToAdjacencyVectorIndex(originalMatrixData, row, col + 1)
            adjacencyMatrixBuilder.add(adjacencyRow, adjacencyCol, 1)
          }
        }

        if (row > 0) { //if we have a candidate from the left
          val candidate = originalMatrixData(row - 1, col)
          if (original > candidate) {
            val adjacencyCol = rowAndColToAdjacencyVectorIndex(originalMatrixData, row - 1, col)
            adjacencyMatrixBuilder.add(adjacencyRow, adjacencyCol, 1)
          }
        }

        if (row < originalMatrixData.rows - 1) { //if we have a candidate from the right
          val candidate = originalMatrixData(row + 1, col)
          if (original > candidate) {
            val adjacencyCol = rowAndColToAdjacencyVectorIndex(originalMatrixData, row + 1, col)
            adjacencyMatrixBuilder.add(adjacencyRow, adjacencyCol, 1)
          }
        }
    })
    val adjacencyMatrix: CSCMatrix[Int] = adjacencyMatrixBuilder.result
    println("adjacency matrix build complete")
    adjacencyMatrix
  }

  /**
    * Find max length path.
    *
    * @param adjacencyMatrix adjacency matrix.
    * @param prevPathMatrix  previous iteration paths.
    * @param prevIterations  all completed iterations except previous.
    * @return collection of iteration results.
    */
  @tailrec
  def findMatrixPaths(
                       adjacencyMatrix: CSCMatrix[Int],
                       prevPathMatrix: CSCMatrix[Int],
                       prevIterations: List[CSCMatrix[Int]] = List.empty
                     ): List[CSCMatrix[Int]] = {
    println(s"find matrix paths iteration ${prevIterations.size}")
    val iterationMatrix = prevPathMatrix * adjacencyMatrix
    if (iterationMatrix.data.isEmpty) {
      println(s"find matrix paths complete with ${prevIterations.size} iterations")
      prevPathMatrix :: prevIterations
    } else {
      findMatrixPaths(adjacencyMatrix, iterationMatrix, prevPathMatrix :: prevIterations)
    }
  }

  /**
    * Compute height difference between two points in an adjacency vector.
    *
    * @param originalMatrixData original matrix with point heights in it.
    * @param fromIndex          from point.
    * @param toIndex            to point.
    * @return height difference.
    */
  def getHeight(originalMatrixData: Matrix[Int], fromIndex: Int, toIndex: Int): Int = {
    val (fromRow, fromCol) = adjacencyVectorIndexToRowAndCol(originalMatrixData, fromIndex)
    val (toRow, toCol) = adjacencyVectorIndexToRowAndCol(originalMatrixData, toIndex)
    val from = originalMatrixData(fromRow, fromCol)
    val to = originalMatrixData(toRow, toCol)
    val diff = from - to
    diff
  }

  /**
    * Find a path with a max height difference between path start and end points in an original matrix.
    *
    * @param originalMatrixData original matrix with point heights in it.
    * @param pathData           pats matrix.
    * @return (max height difference, List((adjacency vector start index, adjacency vector end index)).
    */
  def findMaxHeightPaths(originalMatrixData: Matrix[Int], pathData: CSCMatrix[Int]): (Int, List[(Int, Int)]) = {
    pathData.activeKeysIterator.foldLeft((0, List.empty[(Int, Int)]))({
      case ((prevHeight, prevPaths), (candidateIndexRow, candidateIndexCol)) =>
        val candidateHeight = getHeight(originalMatrixData, candidateIndexRow, candidateIndexCol)
        if (candidateHeight > prevHeight) {
          (candidateHeight, List((candidateIndexRow, candidateIndexCol)))
        } else if (candidateHeight == prevHeight) {
          (prevHeight, (candidateIndexRow, candidateIndexCol) :: prevPaths)
        } else {
          (prevHeight, prevPaths)
        }
    })
  }

  /**
    * Find all adjacency matrix columns with paths to the row.
    *
    * @param data adjacency matrix.
    * @param row  row to filter.
    * @return found columns.
    */
  def filterColIndexesByRow(data: CSCMatrix[Int], row: Int): Set[Int] = {
    data.activeIterator.collect({
      case ((`row`, col), _) => col
    }).toSet
  }

  /**
    * Find all adjacency matrix rows with path to the column.
    *
    * @param data adjacency matrix.
    * @param col  column to filter.
    * @return found rows.
    */
  def filterRowIndexesByCol(data: CSCMatrix[Int], col: Int): Set[Int] = {
    data.activeIterator.collect({
      case ((row, `col`), _) => row
    }).toSet
  }

//  val sourceMatrix: IndexedSeq[IndexedSeq[String]] = testMatrixData()
  val sourceMatrix: IndexedSeq[IndexedSeq[String]] = matrixData()
  val stringMatrix = sourceMatrix.drop(1)
  val matrix: DenseMatrix[Int] = DenseMatrix.tabulate(stringMatrix.length, stringMatrix.head.length)((i, j) => stringMatrix(i)(j).toInt)

  val adjacencyMatrix = buildAdjacencyMatrix(matrix)

  val iterations = findMatrixPaths(adjacencyMatrix, adjacencyMatrix)
  val pathsCount = iterations.headOption.map(_.activeKeysIterator.size).getOrElse(0)
  println(s"found $pathsCount paths with ${iterations.size} length")

  //build paths from last iteration
  val (height, adjacencyVectorPaths) = iterations.headOption.map(lastIteration => {
    //find all paths with maximum height difference between start and end points in original matrix
    val (maxHeight, maxHeightPaths) = findMaxHeightPaths(matrix, lastIteration)
    println(s"found ${maxHeightPaths.size} possible directions with length ${iterations.size} and height $maxHeight")
    //group paths by start point
    val paths: Iterable[List[Int]] = maxHeightPaths.groupBy(_._1).mapValues(_.map(_._2)).flatMap({
      case (row, cols) =>
        val initialData: Map[Int, Iterable[List[Int]]] = cols.map(col => (col, Iterable(List.empty))).toMap
        //group paths by start point
        val rowColPaths = iterations.tail.foldLeft(initialData)({
          case (prevIterationData, iterationMatrix) =>
            val iterationPaths: Map[Int, Iterable[List[Int]]] = prevIterationData.toIterable.flatMap({
              //for all path ends look for points for which there is a path from start to a point and a path of length 1 from the point to end
              case (targetCol, targetColPaths) =>
                val nextColPaths: Iterable[List[Int]] = targetColPaths.map(targetColPath => targetCol :: targetColPath)
                //look for points for which there is a path from start to a point
                val rowCandidates = filterColIndexesByRow(iterationMatrix, row)
                //look for points for which there is a path of length 1 from a point to end
                val colCandidates = filterRowIndexesByCol(adjacencyMatrix, targetCol)
                val nextCols = rowCandidates intersect colCandidates
                nextCols.map(nextCol => (nextCol, nextColPaths))
            }).groupBy(_._1).mapValues(_.flatMap(_._2))
            iterationPaths
        })
        val rowPaths: Iterable[List[Int]] = rowColPaths.flatMap({
          case (lastCol, lastColPaths) =>
            lastColPaths.map(lastColPath => row :: lastCol :: lastColPath)
        })
        rowPaths
    })
    (maxHeight, paths)
  }).getOrElse((0, Iterable.empty))
  //transform a path between adjacency vector positions to a path between original matrix points
  val paths = adjacencyVectorPaths.zipWithIndex.map({
    case (adjacencyVectorPath, index) =>
      val matrixPath = adjacencyVectorPath.map(
        adjacencyVectorIndex => adjacencyVectorIndexToRowAndCol(matrix, adjacencyVectorIndex)
      )
      val matrixPathHeights = matrixPath.map(pint => matrix(pint._1, pint._2))
      (index, matrixPath, matrixPathHeights)
  })
  paths.foreach({
    case (index, path, pathHeights) =>
      println(s"path $index: $path")
      println(s"path $index heights: $pathHeights")
  })
}
