package fr.claravista.shark.udf

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{ListObjectInspector, ObjectInspector}
import scala.collection.JavaConversions._

/**
 * This generic udf is for joining two generic Arrays.
 * @author coderh
 */

class GenericConcat2Arrays extends GenericUDF {

  def evaluate(args: Array[GenericUDF.DeferredObject]): Object = {
    val arrayJoined = (listOIs zip args.init map (p => p._1.getList(p._2.get).toList)).flatten
    if (booleanOI.get(args.last.get)) arrayJoined.distinct else arrayJoined
  }

  def getDisplayString(children: Array[String]): String = {
    "Concat " + children(0) + " and " + children(1)
  }

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {

    // test arguments number
    if (arguments.length != 3) {
      throw new UDFArgumentLengthException("The operator 'Concat2Arrays' accepts 3 arguments.")
    }

    // distinct flag
    booleanOI = arguments.last.asInstanceOf[BooleanObjectInspector]

    // first 2 arguments
    listOIs = arguments.init.map(_.asInstanceOf[ListObjectInspector])

    /**
     * ReturnObjectInspectorResolver helps to find the return ObjectInspector for a GenericUDF.
     * This class will help detect whether all possibilities have exactly the same ObjectInspector.
     * If not, then we need to convert the Objects to the same ObjectInspector.
     *
     * In shark, we can not concat array<A> and array<B>
     */

    // test if the two input arrays have the same type.
    val returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true)
    if (!(returnOIResolver.update(arguments(0)) && returnOIResolver.update(arguments(1)))) {
      throw new UDFArgumentTypeException(2, "The 1st and 2nd args of function Concat2Arrays should have the same type, " + "but they are different: \"" + arguments(0).getTypeName + "\" and \"" + arguments(1).getTypeName + "\"")
    }
    returnOIResolver.get
  }

  private var listOIs: Array[ListObjectInspector] = null
  private var booleanOI: BooleanObjectInspector = null
}