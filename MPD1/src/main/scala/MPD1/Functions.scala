package MPD1

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
//import co.theasi.plotly._


object Functions {
  var jst:Long=0
  var jet:Long=0
  var nsga3st:Long=0
  var nsga3et:Long=0
  def createdDS(f:String):Ws={
   var tempStr = f.split(",");
   var wsTemp: Ws = new Ws(tempStr(0).toString(),tempStr(2).toFloat, tempStr(3).toFloat, tempStr(4).toFloat, tempStr(5).toFloat, tempStr(6).toFloat, tempStr(7).toFloat);
    wsTemp.stypid=tempStr(1)
    if(wsTemp.security==0)
      wsTemp.security=Float(1.4E-44)
    if(wsTemp.reliability==0)
      wsTemp.reliability=Float(1.4E-44)
    if(wsTemp.availability==0)
      wsTemp.availability=Float(1.4E-44)

   return wsTemp

 }
  def factorial(n: Int): Int = {
    if (n == 0)
      return 1
    else
      return n * factorial(n-1)
  }
  def getPoints(upb: Float,n:Int,sc:SparkContext):Array[Ws]={

    var Zs=new Array[Ws](n)
    var l=upb/MPD1.Constnts.divisions
    var i=0
    while(i<n){
      Zs(i)=new Ws("struct",0,0,0,0,0,0)
      i=i+1
    }

    var ZsP=sc.parallelize(Zs,6)
    var ZsM=ZsP.map(f=>crep(f,l)).collect()

    return ZsM
  }

  def crep(f:Ws,l:Float):Ws={

    f.cost=f.cost+(MPD1.NSGAU.keygen(MPD1.Constnts.divisions,1)*l)
    f.time=f.time+(MPD1.NSGAU.keygen(MPD1.Constnts.divisions,1)*l)
    f.customAttributes=f.customAttributes+(MPD1.NSGAU.keygen(MPD1.Constnts.divisions,1)*l)
    f.security=f.security+(MPD1.NSGAU.keygen(MPD1.Constnts.divisions,1)*l)
    f.reliability=f.reliability+(MPD1.NSGAU.keygen(MPD1.Constnts.divisions,1)*l)
    f.availability=f.availability+(MPD1.NSGAU.keygen(MPD1.Constnts.divisions,1)*l)


    return f
  }

  def plotGraphPlotty(x:Array[Array[Ws]],pname:String)={
    /*  implicit val server = new writer.Server {
      val credentials = writer.Credentials("avik.dutta111191", "tnzYCzG5TJka6qPOnKly")
      val url = "https://api.plot.ly/v2/"
    }
    var c=x.map(f=>(MPD1.NSGAU.aggrigateSum(f,1)._1.toDouble))
    var t=x.map(f=>(MPD1.NSGAU.aggrigateSum(f,2)._1.toDouble))

    val p = Plot().withScatter(c.toIterable,t.toIterable,ScatterOptions().mode(ScatterMode.Marker).name("Initial"))

    draw(p, pname)*/
  }

  def keygenRanFloat():Float={
    val r = new scala.util.Random
    val r1 = r.nextFloat()

    return r1
  }

  /*def plotGraphBreeze(x:Array[Array[Ws]])={
    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(0.0,1.0)
    p += plot(x, x :^ 2.0)
    p += plot(x, x :^ 3.0, '.')
    p.xlabel = "x axis"
    p.ylabel = "y axis"
    f.saveas("lines.png")
  }*/

  def computeDistange(f: Array[Ws], Jopt: Broadcast[Ws]): (Float,Array[Ws])={
    var cost :Float =MPD1.NSGAU.aggrigateSum(f,1)._1
    var time :Float  =MPD1.NSGAU.aggrigateSum(f,2)._1
    var reliability :Float =MPD1.NSGAU.aggrigatePro(f,1)._1
    var availability :Float =MPD1.NSGAU.aggrigatePro(f,2)._1
    var security: Float =MPD1.NSGAU.aggrigatePro(f,3)._1
    var customAttributes :Float=MPD1.NSGAU.aggrigateSum(f,3)._1
    var sum1=Math.pow((Jopt.value.cost-cost),2)
    var sum2=Math.pow((Jopt.value.time-time),2)
    var sum3=Math.pow((Jopt.value.customAttributes-customAttributes),2)
    var sum4=Math.pow((Jopt.value.reliability-reliability),2)
    var sum5=Math.pow((Jopt.value.availability-availability),2)
    var sum6=Math.pow((Jopt.value.security-security),2)


    var dist=Math.pow((sum1+sum2+sum3+sum4+sum5+sum6),(1/2))
    return (dist.toFloat,f)

  }


}

