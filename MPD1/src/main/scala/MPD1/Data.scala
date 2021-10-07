package MPD1

import java.util.UUID.randomUUID;
class Ws (typ:String,c:Float,t:Float,r:Float,a:Float,s:Float,cu:Float) extends Serializable {
  var styp=typ;
  var stypid="generic";

  var uid=randomUUID().toString;
  var cost :Float =c;
  var time :Float  =t;
  var reliability :Float =r;
  var availability :Float =a;
  var security: Float =s
  var customAttributes :Float=cu;


  def Ws(typ:String,wsid:String,c:Float,t:Float,r:Float,a:Float,s:Float,cu:Float){
    this.styp=typ;
    this.stypid=wsid

    this.uid=randomUUID().toString;
    this.cost =c;
    this.time =t;
    this.reliability =r;
    this.availability =a;
    this.security=s;
    this.customAttributes=cu;
  }

  def Ws(){
    this.styp="unknown";
    this.uid=randomUUID().toString;
    this.cost =Float.MaxValue;
    this.time =Float.MaxValue;
    this.reliability =Float(1.4E-44);
    this.availability =Float(1.4E-44);
    this.security=Float(1.4E-44);
    this.customAttributes=Float.MaxValue;
  }

  def Ws(typ:String,c:Float){
    this.styp=typ;
    this.uid=randomUUID().toString;
    this.cost =c;
    this.time =Float.MaxValue;
    this.reliability =Float(1.4E-44);
    this.availability =Float(1.4E-44);
    this.security=Float(1.4E-44);
    this.customAttributes=Float.MaxValue;
  }

  def Ws(typ:String,c:Float,t:Float){
    this.styp=typ;
    this.uid=randomUUID().toString;
    this.cost =c;
    this.time =t;
    this.reliability =Float(1.4E-44);
    this.availability =Float(1.4E-44);
    this.security=Float(1.4E-44);
    this.customAttributes=Float.MaxValue;
  }

  def Ws(typ:String,c:Float,t:Float,r:Float){
    this.styp=typ;
    this.uid=randomUUID().toString;
    this.cost =c;
    this.time =t;
    this.reliability =r;
    this.availability =Float(1.4E-44);
    this.security=Float(1.4E-44);
    this.customAttributes=Float.MaxValue;
  }

  def Ws(typ:String,c:Float,t:Float,r:Float,a:Float){
    this.uid=randomUUID().toString;
    this.cost =c;
    this.time =t;
    this.reliability =r;
    this.availability =a;
    this.security=Float(1.4E-44);
    this.customAttributes=Float.MaxValue;
  }

  def Ws(typ:String,c:Float,t:Float,r:Float,a:Float,s:Float){
    this.styp=typ;
    this.uid=randomUUID().toString;
    this.cost =c;
    this.time =t;
    this.reliability =r;
    this.availability =a;
    this.security=s;
    this.customAttributes=Float.MaxValue;
  }

  def WsRand(){
    this.styp="rand";
    this.uid=randomUUID().toString;
    this.cost =MPD1.Functions.keygenRanFloat();
    this.time =MPD1.Functions.keygenRanFloat();
    this.reliability =MPD1.Functions.keygenRanFloat();
    this.availability =MPD1.Functions.keygenRanFloat();
    this.security=MPD1.Functions.keygenRanFloat();
    this.customAttributes=MPD1.Functions.keygenRanFloat();
  }
  def display()={
    println("STyp : "+styp+" | stypid : "+stypid+"| UID : "+uid+"|"+"cost :"+cost+"|"+"time :"+time+"|"+"reliability :"+reliability+"|"+"availability :"+availability+"|"+"security :"+security+"|"+"customAttributes :"+customAttributes+"|");
  }
}
