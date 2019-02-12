/*
This code gets MIMIC-III discharge summaries in a form of a csv file and extracts QUIs of disease and disorders
and writes it to a csv file called "WithCuis_P". Before using discharge summaries we need to get rid of \n and \r,
this is done in Python.
 */
import scala.collection._
import org.apache.ctakes.typesystem.`type`.refsem.UmlsConcept
import org.apache.ctakes.typesystem.`type`.textsem.DiseaseDisorderMention
import org.apache.uima.fit.factory.{AnalysisEngineFactory, CollectionReaderFactory, JCasFactory}
import org.apache.uima.fit.pipeline.SimplePipeline
import org.apache.uima.fit.util.JCasUtil
import org.apache.uima.jcas.JCas
import scala.io._
import java.io._
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter


class QuickAE8 extends org.apache.uima.fit.component.JCasAnnotator_ImplBase{
  override def process(aJCAS:JCas): Unit = {

    val all = JCasUtil.selectAll(aJCAS)
    val diseaseOrDisorders = JCasUtil.select(aJCAS, classOf[DiseaseDisorderMention])
    val disorderTUI = List("T019", "T020", "T037", "T046", "T047", "T048", "T049", "T050", "T190", "T191", "T184", "T033")
    val diseaseOrDisorders_array = diseaseOrDisorders.toArray(new Array[DiseaseDisorderMention](0))
    var set = scala.collection.mutable.Set[String]()
    var writer = new PrintWriter(new File("output.txt"))
    for(d <- diseaseOrDisorders_array) {
      var umlsconcept = JCasUtil.select(d.getOntologyConceptArr(), classOf[UmlsConcept])
      var umlsconcept_array = umlsconcept.toArray(new Array[UmlsConcept](0))
      var p = d.getPolarity()
      //var c = d.getUncertainty()

      for (con <- umlsconcept_array) {
        if (disorderTUI.contains(con.getTui)) {
          if (p == 1) set.add(con.getCui)
          if (p == -1) set.add(con.getCui + "NegP")
        }
      }
    }
    for (con <- set) {
      writer.write(con)
      writer.write("\n")
      println(con)
    }
    writer.close()
  }
}


object TestAE9 extends App {

  val jCas = JCasFactory.createJCas
  val ae = AnalysisEngineFactory.createEngine(
    "desc.ctakes-clinical-pipeline.desc.analysis_engine.AggregatePlaintextUMLSProcessor"
  )
  val qae = AnalysisEngineFactory.createEngine(
    classOf[QuickAE8]
  )

  def get_cui(text: String): mutable.Set[String] = {
    jCas.reset()
    jCas.setDocumentText(text)
    SimplePipeline.runPipeline(jCas, ae, qae)
    var set2 = scala.collection.mutable.Set[String]()

    for (line <- Source.fromFile("output.txt").getLines) {
      set2.add(line)
    }
    return set2
  }
  val bufferedSource = io.Source.fromFile("Prepared_Data_Clean_Text.csv")
  val outputFile = "WithCuis_P.csv"
  val csvWriter = new CSVWriter(new FileWriter(outputFile,true))
  csvWriter.writeNext ("SUBJECT_ID_T", "HADM_ID_T", "CUIS")

  for (line <- bufferedSource.getLines) {
      var cols = line.split(",").map(_.trim)
      var Cuis = get_cui(cols(11))
      csvWriter.writeNext (s"${cols(1)}" , s"${cols(2)}", Cuis.mkString(" "))
  }
  bufferedSource.close()
  csvWriter.close()
}
