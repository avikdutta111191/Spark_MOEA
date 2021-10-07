package MPD1
import java.io
import java.time.zone.ZoneOffsetTransitionRule
import java.util.UUID.randomUUID

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._
object Jaya {



  def jayaRun(x:Array[Array[Ws]], sc:SparkContext):Ws={

    //var initialPopulation=new Array[Ws](MPD1.Constnts.generatio)

    //initialPopulation.foreach(f=>{f.WsRand()})

   MPD1.Functions.jst= System.nanoTime()

    var initialPopulation=sc.parallelize(x)
    var evalFunc=initialPopulation.map(f=>qosComp(f))
    var g=0
    while(g< MPD1.Constnts.jgeneratio) {
      var Populatio=evalFunc.map(f=>multiObjectiveReduction(f))
      var eval = Populatio.map(f =>(evalJayaFitness(f),f._1)).groupByKey()
      var best = eval.sortByKey().first()
      var worst = eval.sortByKey(false).first()

      var newPopulation = eval.map(f => generateNewPopulation(f, best, worst))

      evalFunc=newPopulation.flatMap(f=>{for(i1<-f) yield{(i1)}})

      g=g+1

    }
    MPD1.Functions.jet= System.nanoTime()

    var Op:Ws=evalFunc.map(f=>(evalJayaFitness(f),f)).sortByKey(true).first()._2



    return  Op
  }


  def qosComp(x:Array[Ws]):Ws={
    var wsE=new  Ws("qos",0.toFloat,0.toFloat,0.toFloat,0.toFloat,0.toFloat,0.toFloat)
    wsE.cost=MPD1.NSGAU.aggrigateSum(x,1)._1
    wsE.time=MPD1.NSGAU.aggrigateSum(x,2)._1
    wsE.customAttributes=MPD1.NSGAU.aggrigateSum(x,3)._1
    wsE.reliability=MPD1.NSGAU.aggrigatePro(x,1)._1
    wsE.availability=MPD1.NSGAU.aggrigatePro(x,2)._1
    wsE.security=MPD1.NSGAU.aggrigatePro(x,3)._1

    return wsE

  }

  def multiObjectiveReduction(f:Ws):(Ws,((Float,Float),(Float,Float)))={

    var jo1:Float=f.cost+f.time+f.customAttributes
    var jo2:Float=f.reliability+f.availability+f.security

    var jo1min=Float(1.4E-44)
    var jo2max=(1*100).toFloat


    return (f,((jo1,jo1min),(jo2,jo2max)))




  }

  def evalJayaFitness(f: (Ws, ((Float, Float), (Float, Float)))):Float= {
    var w1=MPD1.Functions.keygenRanFloat()
    var w2=1-w1
    var c1=0
    var c2=0

    if((f._1.cost+f._1.time+f._1.customAttributes)>=(3*100))
      c1=1
    else if((f._1.cost+f._1.time+f._1.customAttributes)<0)
      c1=(-1)
    if((f._1.reliability+f._1.availability+f._1.security)<0)
      c2=1
    if((f._1.reliability+f._1.availability+f._1.security)>=(3*1))
      c2=(-1)

    var Csum=MPD1.Constnts.jpenalty*c1*c2

    var f1=w1*(f._2._1._1/f._2._1._2)
    var f2=w2*(f._2._2._1/f._2._2._2)


    return (f1-f2+Csum).toFloat


  }

  def evalJayaFitness(f: Ws):Float= {
    var w1=MPD1.Functions.keygenRanFloat()
    var w2=1-w1
    var c1=0
    var c2=0

    if((f.cost+f.time+f.customAttributes)>=(3*100))
      c1=1
    else if((f.cost+f.time+f.customAttributes)<0)
      c1=(-1)
    if((f.reliability+f.availability+f.security)<0)
      c2=1
    if((f.reliability+f.availability+f.security)>=(3*1))
      c2=(-1)

    var Csum=MPD1.Constnts.jpenalty*c1*c2

    var f1=w1*(c1/Float(1.4E-44))
    var f2=w2*(c2/Float.MaxValue)


    return (f1-f2+Csum).toFloat


  }

  def generateNewPopulation(f: (Float, Iterable[Ws]), b: (Float, Iterable[Ws]), w: (Float, Iterable[Ws])): Array[Ws] = {
    var best = b._2.last
    var worst = w._2.last
    var ll = new ListBuffer[Ws]()
    for (x1 <- f._2) {
      var newpop:Ws = new MPD1.Ws("newP",0,0,0,0,0,0)

      newpop.cost=x1.cost+(MPD1.Functions.keygenRanFloat()*(best.cost-math.abs(x1.cost)))-(MPD1.Functions.keygenRanFloat()*(worst.cost-math.abs(x1.cost)))
      newpop.time=x1.time+(MPD1.Functions.keygenRanFloat()*(best.time-math.abs(x1.time)))-(MPD1.Functions.keygenRanFloat()*(worst.time-math.abs(x1.time)))
      newpop.customAttributes=x1.customAttributes+(MPD1.Functions.keygenRanFloat()*(best.customAttributes-math.abs(x1.customAttributes)))-(MPD1.Functions.keygenRanFloat()*(worst.customAttributes-math.abs(x1.customAttributes)))
      newpop.reliability=x1.reliability+(MPD1.Functions.keygenRanFloat()*(best.reliability-math.abs(x1.reliability)))-(MPD1.Functions.keygenRanFloat()*(worst.reliability-math.abs(x1.reliability)))
      newpop.availability=x1.availability+(MPD1.Functions.keygenRanFloat()*(best.availability-math.abs(x1.availability)))-(MPD1.Functions.keygenRanFloat()*(worst.availability-math.abs(x1.availability)))
      newpop.security=x1.security+(MPD1.Functions.keygenRanFloat()*(best.security-math.abs(x1.security)))-(MPD1.Functions.keygenRanFloat()*(worst.security-math.abs(x1.security)))

      ll+=newpop


    }
    return (ll.toList).toArray

  }



}
