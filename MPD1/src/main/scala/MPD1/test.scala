  package MPD1

  import org.apache.spark.{SparkConf, SparkContext}

  import scala.collection.mutable.ListBuffer
  import java.lang.Exception

  import MPD1.NSGAU.aggrigateSum
  import org.apache.spark.broadcast.Broadcast
  object test {


    def main(args: Array[String]) {
      try {
          val conf = new SparkConf().setAppName("Simple Application")
          val sc = new SparkContext(conf)

        println(" \nHello World from Scala!\n")
        var wsArray = new Array[MPD1.Ws](10000)
        var i: Int = 0;
        val filename = "/home/nazi/Downloads/file.csv";
        var lines = sc.textFile(filename)
        var rddWsAll=lines.map(f=>Functions.createdDS(f));
        var alWs=rddWsAll.map(f=>(f.styp,f))
        var  hashalWs = alWs.groupByKey().map(rec=>(rec._1,rec._2));
        println("\n working 3 \n")

        var DuplicateHashalWs = hashalWs;

        var listcomWs: List[List[Ws]] = List();
        var ll = new ListBuffer[List[Ws]]()
        var population =Array.ofDim[MPD1.Ws](MPD1.Constnts.populationSize,MPD1.Constnts.serviceLength);
        var j = 0;
        while (j < MPD1.Constnts.populationSize) {
          i=0;
          hashalWs.collect().foreach(t => {
            var temp=t._2.toArray
            population(j)(i)= temp(MPD1.NSGAU.keygen(MPD1.Constnts.populationSize))
            //t._2.dropRight(1);
            i = i + 1
          })
          j=j+1;
        }



        var lel=population
      // MPD1.Functions.plotGraphPlotty(lel,"Initial Population")

       var x= NSGAU.nsgaFun(lel,sc);
          var p=MPD1.Jaya.jayaRun(x,sc)
          var Jopt = sc.broadcast(p);
          var nsgaOpt = sc.parallelize(x)

          var distpair=nsgaOpt.map(f=>MPD1.Functions.computeDistange(f,Jopt)).groupByKey()
          //lel.foreach(f=>f.foreach(q=>{print(q.styp+" :"+q.uid + "\n") ;print(" \n")}))
        x.foreach(f=>{
          print(MPD1.NSGAU.aggrigateSum(f,1)._1+","+MPD1.NSGAU.aggrigateSum(f,2)._1+","+MPD1.NSGAU.aggrigateSum(f,3)._1+","+MPD1.NSGAU.aggrigatePro(f,1)._1+","+MPD1.NSGAU.aggrigatePro(f,1)._1+","+MPD1.NSGAU.aggrigatePro(f,1)._1+","+1)
          println("\n ........ \n")

        })
        println("\n list \n")


      }
      catch {
        case e: javax.script.ScriptException => e.printStackTrace
      }

    }

  }
