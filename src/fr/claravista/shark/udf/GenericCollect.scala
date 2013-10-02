package fr.claravista.shark.udf

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.{AggregationBuffer, Mode}
import java.util
import fr.claravista.shark.udf.GenericCollect.CollectAllEvaluator.ArrayAggregationBuffer

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 9/30/13
 * Time: 4:45 PM
 */
object GenericCollect {

  object CollectAllEvaluator {
    class ArrayAggregationBuffer extends AggregationBuffer {
      var container: util.ArrayList[AnyRef] = null
    }
  }

  class CollectAllEvaluator extends GenericUDAFEvaluator {
    override def init(m: GenericUDAFEvaluator.Mode, parameters: Array[ObjectInspector]): ObjectInspector = {
      super.init(m, parameters)
      if (m == Mode.PARTIAL1) {
        inputOI = parameters(0)
        ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI))
      }
      else {
        if (!parameters(0).isInstanceOf[StandardListObjectInspector]) {
          inputOI = ObjectInspectorUtils.getStandardObjectInspector(parameters(0))
          ObjectInspectorFactory.getStandardListObjectInspector(inputOI)
        }
        else {
          internalMergeOI = parameters(0).asInstanceOf[StandardListObjectInspector]
          inputOI = internalMergeOI.getListElementObjectInspector
          loi = ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI).asInstanceOf[StandardListObjectInspector]
          loi
        }
      }
    }

    def reset(ab: GenericUDAFEvaluator.AggregationBuffer) {
      ab.asInstanceOf[ArrayAggregationBuffer].container = new util.ArrayList[AnyRef]
    }

    def getNewAggregationBuffer: GenericUDAFEvaluator.AggregationBuffer = {
      val ret: ArrayAggregationBuffer = new ArrayAggregationBuffer
      reset(ret)
      ret
    }

    def iterate(ab: GenericUDAFEvaluator.AggregationBuffer, parameters: Array[AnyRef]) {
      assert(parameters.length == 1)
      val p: AnyRef = parameters(0)
      if (p != null) {
        val agg: ArrayAggregationBuffer = ab.asInstanceOf[ArrayAggregationBuffer]
        agg.container.add(ObjectInspectorUtils.copyToStandardObject(p, this.inputOI))
      }
    }

    def terminatePartial(ab: GenericUDAFEvaluator.AggregationBuffer): AnyRef = {
      val agg: ArrayAggregationBuffer = ab.asInstanceOf[ArrayAggregationBuffer]
      val ret: util.ArrayList[AnyRef] = new util.ArrayList[AnyRef](agg.container.size)
      ret.addAll(agg.container)
      ret
    }

    def merge(ab: GenericUDAFEvaluator.AggregationBuffer, o: AnyRef) {
      val agg: ArrayAggregationBuffer = ab.asInstanceOf[ArrayAggregationBuffer]
      @SuppressWarnings(Array("unchecked")) val partial: util.ArrayList[AnyRef] = internalMergeOI.getList(o).asInstanceOf[util.ArrayList[AnyRef]]
      import scala.collection.JavaConversions._
      for (i <- partial) {
        agg.container.add(ObjectInspectorUtils.copyToStandardObject(i, this.inputOI))
      }
    }

    def terminate(ab: GenericUDAFEvaluator.AggregationBuffer): AnyRef = {
      val agg: ArrayAggregationBuffer = ab.asInstanceOf[ArrayAggregationBuffer]
      val ret: util.ArrayList[AnyRef] = new util.ArrayList[AnyRef](agg.container.size)
      ret.addAll(agg.container)
      ret
    }

    private var inputOI: ObjectInspector = null
    private var loi: StandardListObjectInspector = null
    private var internalMergeOI: StandardListObjectInspector = null
  }

}

class GenericCollect extends AbstractGenericUDAFResolver {
  override def getEvaluator(tis: Array[TypeInfo]): GenericUDAFEvaluator = {
    if (tis.length != 1) {
      throw new UDFArgumentTypeException(tis.length - 1, "Exactly one argument is expected.")
    }
    new GenericCollect.CollectAllEvaluator
  }
}