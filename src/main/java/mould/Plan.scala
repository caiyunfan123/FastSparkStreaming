package mould

trait Plan[K,V] extends Serializable{
  def reduceByKeyMethod(v1:Any,v2:Any):Any=reduceByKey(v1.asInstanceOf[V],v2.asInstanceOf[V])
  def windowMehthod(k:Any,v:Any):Unit=window(k.asInstanceOf[K],v.asInstanceOf[V])

  def mapMethod(value:String):(Any,Any)
  protected def reduceByKey(v1:V, v2:V):V
  protected def window(k:K, v:V):Unit
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