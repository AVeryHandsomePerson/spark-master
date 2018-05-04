package com.cn.carenet.function

object Sortingmethod {
  /**
    * 1、冒泡排序
    */
  def compute(data: Int, dataSet: List[Int]): List[Int] = dataSet match {
    case List() => List(data)
    case head :: tail => if (data <= head) data :: dataSet else head :: compute(data, tail)
  }
  /**
    * 2、归并排序
    */
  def mergedSort[T](less: (T, T) => Boolean)(list: List[T]): List[T] = {
    def merged(xList: List[T], yList: List[T]): List[T] = {
      (xList, yList) match {
        case (Nil, _) => yList
        case (_, Nil) => xList
        case (x :: xTail, y :: yTail) => {
          if (less(x, y)) x :: merged(xTail, yList)
          else
            y :: merged(xList, yTail)
        }
      }
    }
    val n = list.length / 2
    if (n == 0) list
    else {
      val (x, y) = list splitAt n
      merged(mergedSort(less)(x), mergedSort(less)(y))
    }
  }

  /**
    * 3、快速排序
    * @param arr 输入数组
    * @return 快速排序后的数组
    */
  def quickSort(arr: Array[Double]): Array[Double] = {
    if (arr.length <= 1)
      arr
    else {
      val index = arr(arr.length / 2)
      Array.concat(
        quickSort(arr filter (index >_)),
        arr filter (_ == index),
        quickSort(arr filter (index < _))
      )
    }
  }


}
