import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 用户自定义聚合函数
  * UDF在sql中的计算顺序是先执行from，列出from中的所有数据，然后join、on、where、group by、UDF。
  *   使用group by后，select后面只能查group by的列和聚合函数(也就是返回一个值的函数)，但聚合函数的入参可以是from中的列
  *   因为每个组只有一行
  */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{
  /*
  表示输入只有一个参数，占用buffer的一个格子，是个string类型的，变量名叫cityInfo
    StructType的参数必须是集合、数组、seq
   */
  override def inputSchema: StructType = StructType(StructField("cityInfo",StringType)::Nil)
  /*
   表示输入的参数经过update的计算后，得到的结果和结果的类型
     入参先经过update的计算，一般跟buffer原有的值进行聚合，然后再放到update中，此方法就行用来规范update的返回值
   */
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo",StringType)::Nil)
  /*
   evaluate的返回值
   */
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true
  /*
  初始化buffer的值，buffer的第一个值并不是传进来的，而是使用此方法初始化的
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = ""

  /*
  将入参和buffer中的当前值计算然后再放回到buffer的对应格子中
  */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
    var bufferCityInfo = buffer(0).toString
    var inputStr = input.getString(0)

    if (!bufferCityInfo.contains(inputStr)) {
      if ("".equals(bufferCityInfo)) bufferCityInfo = inputStr else  buffer(0) += ("," + inputStr)
      buffer.update(0, bufferCityInfo)
    }
  }


  /*
  将多个UDAF的返回值reduce聚合
  */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0);
    val bufferCityInfo2 = buffer2.getString(0);

    for(cityInfo <- bufferCityInfo2.split(",")) {
      if(!bufferCityInfo1.contains(cityInfo)) {
        if("".equals(bufferCityInfo1)) {
          bufferCityInfo1 += cityInfo;
        } else {
          bufferCityInfo1 += "," + cityInfo;
        }
      }
    }

    buffer1.update(0, bufferCityInfo1);
  }
  /*
  update计算完，说明数据源已经遍历完，此时求结果
  */
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
