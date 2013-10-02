package fr.claravista.shark.udf

import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, IntObjectInspector, StringObjectInspector}
import org.apache.hadoop.hive.ql.exec.{UDFArgumentLengthException, UDFArgumentTypeException}
import org.apache.hadoop.io.{IntWritable, MapWritable, LongWritable, Text}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.`lazy`.{LazyInteger, LazyString}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.Days
import scala.Predef.String
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 9/26/13
 * Time: 5:11 PM
 */

class GenericUDFNavigationNotes extends GenericUDF {

  // arguments objectInspectors
  private var visitListOI: ListObjectInspector = null
  private var pageInfoListOI: ListObjectInspector = null
  private var currentDateOI: StringObjectInspector = null
  private var lastDaysOI: IntObjectInspector = null
  private var lastWeeksOI: IntObjectInspector = null
  private var lastMonthsOI: IntObjectInspector = null

  // inner objectInspectors
  private var visitStructOI: StructObjectInspector = null
  private var pageInfoStructOI: StructObjectInspector = null
  private var idCategoryOI: IntObjectInspector = null
  private var freqOI: IntObjectInspector = null
  private var dateVisitOI: StringObjectInspector = null

  /**
   * This is what we do in the initialize() method:
   * Verify that the input is of the type expected
   * Set up the ObjectInspectors for the input in global variables
   * Initialize the output ArrayList
   * Return the ObjectInspector for the output
   */
  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {

    // check number of arguments
    if (arguments.length != 5) throw new UDFArgumentLengthException("GetHeatOfVisit() accepts exactly 5 arguments.")

    // get objectInspector of array<struct<idvisite, datevisite, isauthent, affichages>> and its inner struct
    visitListOI = arguments(0).asInstanceOf[ListObjectInspector]
    visitStructOI = visitListOI.getListElementObjectInspector.asInstanceOf[StructObjectInspector]

    // check the number of struct fields of struct<idvisite, datevisite, isauthent, affichages>
    if (visitStructOI.getAllStructFieldRefs.size != 4) {
      throw new UDFArgumentTypeException(0, "Incorrect number of fields in the struct of array.")
    }

    // get struct field
    val idVisit: StructField = visitStructOI.getStructFieldRef("idvisite")
    val dateVisit: StructField = visitStructOI.getStructFieldRef("datevisite")
    val isAuth: StructField = visitStructOI.getStructFieldRef("isauthent")
    val pageInfo: StructField = visitStructOI.getStructFieldRef("affichages")

    // check if all fields exist
    if (idVisit == null || dateVisit == null || isAuth == null || pageInfo == null)
      throw new UDFArgumentTypeException(0, "No such field in input visit structure " + arguments(0).getTypeName)

    // get all object inspectors of struct field, in which the pageInfo field is an array<struct<page, IdCategorie, freq>>
    dateVisitOI = dateVisit.getFieldObjectInspector.asInstanceOf[StringObjectInspector]
    //    val idVisitOI: ObjectInspector = idVisit.getFieldObjectInspector
    //    val authOI: ObjectInspector = isAuth.getFieldObjectInspector

    // get objectInspector of array<struct<page, IdCategorie, freq>> and that of its inner struct
    pageInfoListOI = pageInfo.getFieldObjectInspector.asInstanceOf[ListObjectInspector]
    pageInfoStructOI = pageInfoListOI.getListElementObjectInspector.asInstanceOf[StructObjectInspector]

    // // check the number of struct fields of array<struct<page, IdCategorie, freq>>
    if (pageInfoStructOI.getAllStructFieldRefs.size != 3) {
      throw new UDFArgumentTypeException(0, "Incorrect number of fields in the struct of array.")
    }

    // get struct field
    val page: StructField = pageInfoStructOI.getStructFieldRef("page")
    val idCategory: StructField = pageInfoStructOI.getStructFieldRef("IdCategorie")
    val freq: StructField = pageInfoStructOI.getStructFieldRef("freq")

    // check if all fields exist
    if (page == null || idCategory == null || freq == null)
      throw new UDFArgumentTypeException(0, "No such field in pageInfo structure " + pageInfoListOI.getTypeName)

    // get all object inspectors of struct field,
    //    val pageOI: ObjectInspector = page.getFieldObjectInspector
    idCategoryOI = idCategory.getFieldObjectInspector.asInstanceOf[IntObjectInspector]
    freqOI = freq.getFieldObjectInspector.asInstanceOf[IntObjectInspector]

    // check the last four primitive arguments
    currentDateOI = arguments(1).asInstanceOf[StringObjectInspector]
    lastDaysOI = arguments(2).asInstanceOf[IntObjectInspector]
    lastWeeksOI = arguments(3).asInstanceOf[IntObjectInspector]
    lastMonthsOI = arguments(4).asInstanceOf[IntObjectInspector]

    val longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector
    val intOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector
    val mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(intOI, intOI)
    val outputFieldNames = Seq("heat", "cate")
    val outputInspectors = Seq(longOI, mapOI)
    ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames, outputInspectors)
  }

  /**
   * The evaluate() method. The input is passed in as an array of DeferredObjects,
   * so that computation is not wasted on deserializing them if they're not actually used
   */
  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    if (arguments.length != 5) return null
    if (arguments(0).get == null || arguments(1).get == null || arguments(2).get == null || arguments(3).get == null || arguments(4).get == null) return null

    val nbElem: Int = visitListOI.getListLength(arguments(0).get)

    // get arguments
    val today: String = currentDateOI.getPrimitiveJavaObject(arguments(1).get)
    val lastDays: Int = lastDaysOI.get(arguments(2).get)
    val lastWeeks: Int = lastWeeksOI.get(arguments(3).get)
    val lastMonths: Int = lastMonthsOI.get(arguments(4).get)

    def dateDiff(strDate: String) = {
      val formatter: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")
      Days.daysBetween(formatter.parseDateTime(strDate), formatter.parseDateTime(today)).getDays
    }


    def getStructFieldElementAt(i: Int, listObject: AnyRef, fieldName: String, structOI: StructObjectInspector, listOI: ListObjectInspector) = {
      structOI.getStructFieldData(listOI.getListElement(listObject, i), structOI.getStructFieldRef(fieldName))
    }


    def visitingDate(i: Int) = {
      getStructFieldElementAt(i, arguments(0).get, "datevisite", visitStructOI, visitListOI) match {
        case dt: LazyString => dateVisitOI.getPrimitiveWritableObject(dt).toString
        case dt: Text => dt.toString
      }
    }

    def cateFreqPair(i: Int) = {
      getStructFieldElementAt(i, arguments(0).get, "affichages", visitStructOI, visitListOI) match {
        case affList: AnyRef => {
          Range(0, visitListOI.getListLength(affList)).map(j => {
            val idCateObj = getStructFieldElementAt(j, affList, "IdCategorie", pageInfoStructOI, pageInfoListOI) match {
              case obj: LazyInteger => obj
              case obj: IntWritable => obj
            }
            val freqObj = getStructFieldElementAt(j, affList, "freq", pageInfoStructOI, pageInfoListOI) match {
              case obj: LazyInteger => obj
              case obj: IntWritable => obj
            }
            (idCategoryOI.get(idCateObj), freqOI.get(freqObj))
          })
        }
      }
    }

    def flatt(p: IndexedSeq[IndexedSeq[(Int, Int)]]): IndexedSeq[(Int, Int)] = {
      p match {
        case IndexedSeq() => IndexedSeq()
        case _ => p.head ++ flatt(p.tail)
      }
    }

    // calculate heat
    val days = Range(0, nbElem).map(visitingDate).map(dateDiff)
    val nbVstLastDays = days.count(d => d < lastDays && d > 0)
    val nbVstLastWeeks = days.count(d => d < 7 * lastWeeks && d > 0)
    val nbVstLastMonths = days.count(d => d < 30 * lastMonths && d > 0)
    val heat = (nbVstLastDays + 1) * (nbVstLastWeeks + 1) * (nbVstLastMonths + 1)

    // calculate idCategory-freq map
    val catMap = new MapWritable()
    val list = flatt(Range(0, nbElem).map(cateFreqPair)).groupBy(_._1).map(p => (p._1, p._2.map(p => p._2).sum))
    list.foreach(p => catMap.put(new IntWritable(p._1), new IntWritable(p._2)))

    // return a object can be parsed by Java
    Array(new LongWritable(heat), catMap)
  }

  def getDisplayString(children: Array[String]): String = {
    if (children.length > 0)
      "GetAllNavigationNotesForEachClient <" + children(0) + ">"
    else
      "<ERROR>"
  }
}