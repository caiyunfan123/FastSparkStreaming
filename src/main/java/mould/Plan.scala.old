package mould

trait Plan[K,V] extends Serializable{
  def mapMethod(value:String):(Any,Any)
  def reduceByKey(v1:V, v2:V):Any
  def window(k:K, v:V):Unit
}
object Plan{
  def apply[K,V](mapFun:String=>(K,V),reduceByKeyFun:(V,V)=>V,windowFun:(K,V)=>Unit):Plan[K,V] =
    new Plan[K,V](){
    override def mapMethod(value: String): (K, V) = mapFun(value)
    override def reduceByKey(v1: V, v2: V): V = reduceByKeyFun(v1,v2)
    override def window(k: K, v: V): Unit = windowFun(k,v)
  }
  def wordCountPlan=apply[String,Int](
    (_,1),(a,b)=>a+b,(a,b)=>println((a,b))
  )
}