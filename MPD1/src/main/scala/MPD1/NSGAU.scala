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


object NSGAU {



  def nsgaFun(x:Array[Array[Ws]], sc:SparkContext):Array[Array[Ws]]={
    var OpSet:Array[Array[Ws]]=new Array[Array[Ws]](MPD1.Constnts.nsga3out)
    breakable {
      var genI=0
      var x1 = x
      var st: List[Array[Ws]] = List()
      var populationParallel = sc.parallelize(x1)

      while (genI <= MPD1.Constnts.generatio) {

        var qt = populationParallel.map(f => (keygen(20), f))
        var qtr = qt.reduceByKey((x, y) => crossover(x, y))
        var rr = qt.union(qtr);
        var rrC = rr.map(f => (f._2)).collect()
        var bcP = sc.broadcast(rrC);
        var sortedList = nondominatiogSort(sc, bcP, rr).collect().toList
        var sortedList1=sortedList
        while (st.length <= MPD1.Constnts.populationSize) {
          st = st.union(sortedList.last._2.toList)
          sortedList = sortedList.dropRight(1)
        }
        var LastFront = sortedList.last

        if (st.length == MPD1.Constnts.populationSize) {
          x1 = st.toArray
          break
        }
        else{
          x1=sortedList1.last._2.toArray
          var Zr:Array[Ws]=Normalize(st.toArray,null,sc)
          var As:RDD[(Ws,Iterable[(Array[Ws],Float)])]=Associate(st.toArray,Zr,sc)

          populationParallel=populationParallel.union(nicePre(As,MPD1.Constnts.k,LastFront,sc))
        }

        genI=genI+1

        var s= populationParallel.map(f=>((MPD1.NSGAU.aggrigateSum(f,1),MPD1.NSGAU.aggrigateSum(f,2),MPD1.NSGAU.aggrigateSum(f,3),MPD1.NSGAU.aggrigatePro(f,1),MPD1.NSGAU.aggrigatePro(f,1),MPD1.NSGAU.aggrigatePro(f,1))))
        s.saveAsTextFile("/home/nazi/nsga1/"+genI+"i"+"_Gen")
      }
        var  i1=0
      populationParallel.collect().take(MPD1.Constnts.nsga3out).foreach(f=>{
        OpSet(i1)=f
        i1=i1+1
      })

    }
    return  OpSet
  }



  def nondominatiogSortComp(x: Array[Ws], y: Array[Ws]): Int = {
    var cost :Float =0;
    var time :Float  =0;
    var reliability :Float =1;
    var availability :Float =1;
    var security: Float =1
    var customAttributes :Float=0;


    var ycost :Float =0;
    var ytime :Float  =0;
    var yreliability :Float =1;
    var yavailability :Float =1;
    var ysecurity: Float =1
    var ycustomAttributes :Float=0;


    for(i<- 0 to 99)
      {
        cost=cost+x(i).cost
        time=time+x(i).time
        reliability=reliability*x(i).reliability
        availability=availability*x(i).availability
        security=(security*x(i).security)
        customAttributes=customAttributes+x(i).customAttributes

        ycost=ycost+y(i).cost
        ytime=ytime+y(i).time
        yreliability=yreliability*y(i).reliability
        yavailability=yavailability*y(i).availability
        ysecurity=(ysecurity*y(i).security)
        ycustomAttributes=ycustomAttributes+y(i).customAttributes
      }

      if(cost>ycost || time>ytime || reliability<yreliability || availability<yavailability || security<ysecurity || customAttributes>ycustomAttributes)
          return 1
      else
          return 0

  }

  def nondominatiogSortMap(bcP: Broadcast[Array[Array[Ws]]], k: Array[Ws]): (Int,Array[Ws])  = {
    var x=0
    bcP.value.foreach(f=>{
      x=x+ nondominatiogSortComp(f,k)
    })
    return (x,k)
  }

  def nondominatiogSort(sc: SparkContext, bcP: Broadcast[Array[(Array[Ws])]], rr: RDD[(Int, Array[Ws])]) : RDD[( Int,Iterable[Array[Ws]])]={
   // var pm=rr.map(f=>nondominatiogSortMap(sc,bcP,f))
    var pm=rr.map(f=>(f._2))
    var pmM=pm.map(k=>(nondominatiogSortMap(bcP,k)))
    var sortedPmM=pmM.groupByKey().sortByKey()


    return sortedPmM
  }


 def  keygen(x:Int):Int={
   val r = new scala.util.Random
   val r1 = 0 + r.nextInt(( x - 1) )

   return r1
 }

  def  keygen(x:Int,y:Int):Int={
    val r = new scala.util.Random
    val r1 = y + r.nextInt(( x - y) )

    return r1
  }

  def keygenRan():Float={
    val r = new scala.util.Random
    val r1 = r.nextFloat()

    return r1
  }

  def crossover(x:Array[Ws],y:Array[Ws]):Array[Ws]={
    var z:Array[Ws]=new Array[Ws](100)
   //var cr= ((x.hashCode().toInt-y.hashCode().toInt)%100)
    for(i<-0 to 99)
      {
        if(MPD1.NSGAU.keygen(100)<=MPD1.Constnts.crossoverRate){
          z(i)=x(i)
        }
        else
          z(i)=y(i)

      }


    return z;
  }

  def Normalize(s:Array[Array[Ws]],zs:Array[Array[Ws]],sc:SparkContext):Array[Ws]={

    var sP=sc.parallelize(s)
    var costF=sP.map(f=>aggrigateSum(f,1)).sortByKey().collect().head._1
    var timeF=sP.map(f=>aggrigateSum(f,2)).sortByKey().collect().head._1
    var customAttributesF=sP.map(f=>aggrigateSum(f,3)).sortByKey().collect().head._1

    var reliabilityF:Float=sP.map(f=>aggrigatePro(f,1)).sortByKey().collect().head._1
    var availabilityF:Float=sP.map(f=>aggrigatePro(f,2)).sortByKey().collect().head._1
    var securityF:Float=sP.map(f=>aggrigatePro(f,3)).sortByKey().collect().head._1


    var cZ=sP.map(f=>(translateOb(f,costF,'c'))).sortByKey().collect()
    var tZ=sP.map(f=>(translateOb(f,timeF,'t'))).sortByKey().collect()
    var cuZ=sP.map(f=>(translateOb(f,customAttributesF,'u'))).sortByKey().collect()
    var rZ=sP.map(f=>(translateOb(f,reliabilityF,'r'))).sortByKey().collect()
    var aZ=sP.map(f=>(translateOb(f,availabilityF,'a'))).sortByKey().collect()
    var sZ=sP.map(f=>(translateOb(f,securityF,'s'))).sortByKey().collect()



    var cZmax=cZ.last._2
    var tZmax=tZ.last._2
    var cuZmax=cuZ.last._2
    var rZmax=rZ.last._2
    var aZmax=aZ.last._2
    var sZmax=sZ.last._2

    var intercepts:Array[Ws]=calIntercepts(cZmax,tZmax,cuZmax,rZmax,aZmax,sZmax)
    var mazInter=intercepts.map(f=>(f.availability+f.security+f.reliability+f.customAttributes+f.time+f.cost)).max
    var nC=MPD1.Constnts.dimesions+(MPD1.Constnts.divisions-1)
    var Cr=MPD1.Constnts.divisions
    var numPoints:Int=(MPD1.Functions.factorial(nC)/(MPD1.Functions.factorial(Cr)*MPD1.Functions.factorial(nC-Cr)))

      var Zs=MPD1.Functions.getPoints(mazInter,numPoints,sc)
    var i=0
    var Zr=new Array[Ws](numPoints)
    Zr=drawPointHyperPlane(Zs,intercepts)

  /*  while(i<numPoints){
      Zr(i)=drawPointHyperPlane(intercepts)
      i=i+1
    }*/
    return Zr
  }

  def aggrigateSum(x:Array[Ws],i:Int): (Float,Array[Ws]) ={
    var agg:Float=0
   if(i==1) {
     for (p <- x) {
       agg = agg + p.cost
     }
   }
   else if(i==2) {
     for (p <- x) {
       agg = agg + p.time
     }
   }
   else if(i==3) {
     for (p <- x) {
       agg = agg + p.customAttributes
     }
   }

    return (agg,x)

  }

  def aggrigatePro(x:Array[Ws],i:Int): (Float,Array[Ws]) ={
    var agg:Float=Float(1.4E-44)
   // var re=p.reliability
    if(i==1) {
      for (p <- x) {
        if(p.reliability==0)
          agg = agg * Float(1.4E-44)
        else
          agg = agg * p.reliability
      }
    }
    else if(i==2) {
      for (p <- x) {
        if(p.availability==0)
          agg = agg * Float(1.4E-44)
        else
          agg = agg * p.availability
      }
    }
    else if(i==3) {
      for (p <- x) {
        if(p.security==0)
          agg = agg *  Float(1.4E-44)
        else
          agg = agg * p.security
      }
    }

    return (agg,x)

  }

  def translateOb(x:Array[Ws],y:Float,z:Char): (Float,Ws)={

    if(z=='c'){
    var WsRp=new Ws("ref",(aggrigateSum(x,1)._1-y),aggrigateSum(x,2)._1,aggrigatePro(x,1)._1,aggrigatePro(x,2)._1,aggrigatePro(x,3)._1,aggrigateSum(x,3)._1)
    return (WsRp.cost,WsRp)
    }
    else if(z=='t'){
      var WsRp=new Ws("ref",aggrigateSum(x,1)._1,(aggrigateSum(x,2)._1-y),aggrigatePro(x,1)._1,aggrigatePro(x,2)._1,aggrigatePro(x,3)._1,aggrigateSum(x,3)._1)
      return (WsRp.time,WsRp)
    }
    else     if(z=='u'){
      var WsRp=new Ws("ref",aggrigateSum(x,1)._1,aggrigateSum(x,2)._1,aggrigatePro(x,1)._1,aggrigatePro(x,2)._1,aggrigatePro(x,3)._1,(aggrigateSum(x,3)._1-y))
      return (WsRp.customAttributes,WsRp)
    }
    else     if(z=='r'){
      var WsRp=new Ws("ref",aggrigateSum(x,1)._1,aggrigateSum(x,2)._1,(aggrigatePro(x,1)._1-y),aggrigatePro(x,2)._1,aggrigatePro(x,3)._1,aggrigateSum(x,3)._1)
      return (WsRp.reliability,WsRp)
    }
    else     if(z=='a'){
      var WsRp=new Ws("ref",aggrigateSum(x,1)._1,aggrigateSum(x,2)._1,aggrigatePro(x,1)._1,(aggrigatePro(x,2)._1-y),aggrigatePro(x,3)._1,aggrigateSum(x,3)._1)
      return (WsRp.availability,WsRp)
    }
    else if(z=='s'){
      var WsRp=new Ws("ref",aggrigateSum(x,1)._1,aggrigateSum(x,2)._1,aggrigatePro(x,1)._1,aggrigatePro(x,2)._1,(aggrigatePro(x,3)._1-y),aggrigateSum(x,3)._1)
      return (WsRp.security,WsRp)
    }
    return null
  }



  def drawPointHyperPlane(Zs:Array[Ws],x:Array[Ws]):Array[Ws]={
    var ac=x(0).cost
    var at=x(1).time
    var acu=x(2).customAttributes
    var ar=x(3).reliability
    var aa=x(4).availability
    var as=x(5).security


    var a=at*acu*ar*as*aa
    var b=ac*acu*ar*as*aa
    var c=ac*at*ar*as*aa
    var d=ac*at*acu*as*aa
    var e=ac*at*acu*ar*aa
    var f=ac*at*acu*ar*as

    var D=((-1)*ac*at*acu*ar*as*aa)



    var Zr=new Array[Ws](Zs.length)
    var i=0
    while(i<Zs.length){
      Zr(i)=copmutePoints(Zs(i),a,b,c,d,e,f,D)
      i=i+1
    }

    return  Zr




  }
  
  def copmutePoints(x:Ws,a:Float,b:Float,c:Float,d:Float,e:Float,f:Float,D:Float):Ws={
    var p=((a*x.cost+b*x.time+c*x.customAttributes+d*x.reliability+e*x.security+f*x.availability+D)/(Math.pow(a,2)+Math.pow(b,2)+Math.pow(c,2)+Math.pow(d,2)+Math.pow(e,2)+Math.pow(f,2)))

    if(p.isNaN)
      p=0.toFloat
    var c_0=x.cost-a*p
    var t_0=x.time-b*p
    var cu_0=x.customAttributes-c*p
    var r_0=x.reliability-d*p
    var s_0=x.security-e*p
    var a_0=x.availability-f*p
    return new Ws("struct",c_0.toFloat,t_0.toFloat,r_0.toFloat,a_0.toFloat,s_0.toFloat,cu_0.toFloat)
  }

 /* def copmutePoints(x:Ws,y:Ws):Array[Ws]={
    var Zr=new Array[Ws](MPD1.Constnts.divisions+1)
    var i=0
    Zr(i)=x
    i=i+1
    Zr(i)=y
    i=i+1
    var mslopCT=(x.time)/(-y.cost)
    var dCT=math.pow((math.pow(y.cost,2)+math.pow(y.cost,2)),(1/2))
    var at1=x.time
    var ac1=y.cost
    while(i<(MPD1.Constnts.divisions-2)){
      var n1CT=(math.pow((at1/ac1),2)+1)
      var n2CT=2*(math.pow(at1,2)/ac1)
      var n3CT=math.pow(at1,2)-math.pow((dCT/MPD1.Constnts.divisions),2)
      var c1CT=roots(n1CT,n2CT,n3CT)

      var cc1CT:Double=0

      if (c1CT.min>0 && c1CT.min<y.cost)
        cc1CT=c1CT.min
      else if(c1CT.max>0 && c1CT.max<y.cost)
        cc1CT=c1CT.max

      var tt1Ct=math.pow((math.pow((dCT/MPD1.Constnts.divisions),2)-math.pow(cc1CT,2)),(1/2))
      ac1=cc1CT.toFloat
      at1=tt1Ct.toFloat
      Zr(i)=new Ws("struct",ac1,at1,0,0,0,0)
      i=i+1
    }

    return  Zr
  }*/

  def roots(a : Double, b : Double, c: Double)= {
    if (b*b-4.0*a*c >= 0) {
      Set(1,-1).map(-b + _ * math.sqrt(b*b-4.0*a*c))
    }else{
      Set()
    }
  }

  def calIntercepts(c:Ws,t:Ws,cu:Ws,r:Ws,a:Ws,s:Ws):Array[Ws]={
    var interce=new Array[Ws](6)
    c.security=0;
    c.availability=0;
    c.reliability=0;
    c.customAttributes=0
    c.time=0
    interce(0)=c
    t.security=0;
    t.availability=0;
    t.reliability=0;
    t.customAttributes=0
    t.cost=0
    interce(1)=t
    cu.security=0;
    cu.availability=0;
    cu.reliability=0
    cu.cost=0
    cu.time=0
    interce(2)=cu

    r.security=0;
    r.availability=0;
    r.customAttributes=0
    r.cost=0
    r.time=0
    interce(3)=r

    s.reliability=0;
    s.availability=0;
    s.customAttributes=0
    s.cost=0
    s.time=0
    interce(4)=s

    a.security=0;
    a.reliability=0;
    a.customAttributes=0
    a.cost=0
    a.time=0
    interce(5)=a

    return interce

  }

  def Associate(st:Array[Array[Ws]], zr:Array[Ws], sc:SparkContext):RDD[(Ws,Iterable[(Array[Ws],Float)])]={

    var exp:String="y=(((y2-y1)/(x2-x1))*(x-x1))+y1"
    var bcZ = sc.broadcast(zr);
    var stp=sc.parallelize(st)
    var stM=stp.map(f=>(associateMap(f,bcZ))).map(t=>associateToZrMap(t)).groupByKey()


    return  stM


  }

  def associateToZrMap(f: Array[(Float, Array[Ws], Ws)]): (Ws,(Array[Ws],Float)) = {
    var min:Float=Float.MaxValue
    var tuple:(Ws,(Array[Ws],Float))=null
      f.foreach(t=>{
        if(t._1<min) {
          min = t._1
          tuple=(t._3,(t._2,min))
        }
      })
    return  tuple
  }

  def associateMap(s:Array[Ws],bcz:Broadcast[Array[Ws]]): (Array[(Float,Array[Ws],Ws)]) ={
    var nC=MPD1.Constnts.dimesions+(MPD1.Constnts.divisions-1)
    var Cr=MPD1.Constnts.divisions
    var p=new Array[(Float,Array[Ws],Ws)]((MPD1.Functions.factorial(nC)/(MPD1.Functions.factorial(Cr)*MPD1.Functions.factorial(nC-Cr))))
    var i=0
    bcz.value.foreach(f=>{
      var dist:Float=compDistance(s,f)
      p(i)=(dist,s,f)
      i=i+1
      })
    return p
  }

  def compDistance(st:Array[Ws],f:Ws): Float = {
    var d:Float=0
    var t1=(-100)
    var c:Float=aggrigateSum(st,1)._1
    var t=aggrigateSum(st,2)._1
    var cu=aggrigateSum(st,3)._1
    var r=aggrigatePro(st,1)._1
    var a=aggrigatePro(st,2)._1
    var s=aggrigatePro(st,3)._1
    var dn=(Math.pow((t1)*f.cost,2)+Math.pow((t1)*f.time,2)+Math.pow((t1)*f.reliability,2)+Math.pow((t1)*f.availability,2)+Math.pow((t1)*f.security,2)+Math.pow((t1)*f.customAttributes,2))
    var nem=(((t1)*f.cost)*c+(t1)*f.time*t+(t1)*f.reliability*r+(t1)*f.availability*a+(t1)*f.security*s+(t1)*f.customAttributes*cu)
    d=(nem.toFloat)/(Math.pow(dn,(1/2)).toFloat)
    return d

  }

  def nicePre(As: RDD[(Ws, Iterable[(Array[Ws], Float)])], K:Int,LastFront: (Int, Iterable[Array[Ws]]),sc:SparkContext): RDD[Array[Ws]] = {
      var pt:ListBuffer[Array[Ws]]=new ListBuffer[Array[Ws]]()
    var k=1
    while (k<K){
      var size=MPD1.Constnts.populationSize/MPD1.Constnts.generatio

      var Jmin=As.map(f=>(f._2.toList.length,f)).sortByKey().collect().take(size)

      var i=keygen(Jmin.length-1,0)
      if(Jmin(i)._1==0){
        var dist:Float=0
        var p1:Array[Ws]=null
        LastFront._2.foreach(f=>{
          var  dis=compDistance(f,Jmin(i)._2._1)
          if(dis<dist)
            {
              p1=f
            }
        })
        pt.+=(p1)

      }
      else{
       var lf1= LastFront._2.toArray
        //var  dis=compDistance(lf1(keygen(lf1.length,0)),Jmin(i)._2._1)
        pt.+=(lf1(keygen(lf1.length,0)))
      }

      k=k+1
    }
    return sc.parallelize(pt.toSeq)

  }






}
