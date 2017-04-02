import numpy as np
import itertools as it
from pyspark.mllib.linalg import Vectors
import random
import msmodel

class LshHash(object):
  def __init__(self):
    pass
    
  def hashfunc(self,x,w,b,tabHash1):
    tabHash = []
    for xx in range(len(tabHash1)):
      sum1 = 0
      for y in range(len(x)):
        sum1 += x[y]*tabHash1[xx][y]
      tabHash.append((sum1+b)/w)
    return np.sum(tabHash)

class Bary0(object):
  """docstring for Bary0"""
  def __init__(self):
    pass

  def bary(self,tab1,k):
    def f1(obj):
      return list(obj[0])
    def f2(obj):
      return obj/k

    def reduceList(l1,l2):
      l3 = []
      for x in range(len(l1)):
        l3.append(l1[x]+l2[x])
      return l3

    tab2 = map(f1,tab1)
    bary1 = reduce(lambda x, y: reduceList(x,y) ,tab2)
    bary1 = map(f2,bary1)
    return Vectors.dense(bary1)
    

class msLsh(object):
  """docstring for msLsh"""
  def __init__(self,
      k,
      threshold_cluster,
      yStarIter,
      cmin,
      normalisation,
      w,
      nbseg,
      nbblocs,
      numPart):
    #super(msLsh, self).__init__()
    self.k = k
    self.threshold_cluster = threshold_cluster
    self.yStarIter = yStarIter
    self.cmin = cmin
    self.normalisation = normalisation
    self.w = w
    self.nbseg = nbseg
    self.nbblocs = nbblocs
    self.numPart = numPart

  def void():
    pass

  def set_numPart(self,np):
    self.numPart = np
    return self

  def set_boolnorm(self,bool1):
    self.normalisation = bool1
    return self

  def set_w(self,ww):
    self.w = ww
    return self

  def set_nbseg(self,nbseg1):
    self.nbseg = nbseg1
    return self

  def set_nbblocs(self,bloc1):
    self.nbblocs = bloc1
    return self

  def set_k(self,kval):
    self.k = kval
    return self


  def set_threshold_cluster(self,threshold_cluster_val):
    self.threshold_cluster = threshold_cluster_val
    return self


  def set_yStarIter(self,yStarIter_val):
    self.yStarIter = yStarIter_val
    return self



  def set_cmin(self,cmin_val):
    self.cmin = cmin_val
    return self	

  def get_k():
    return self.k


  def get_threshold_cluster(self):
    return self.threshold_cluster


  def get_yStarIter(self):
    return self.yStarIter

  def get_cmin(self):
    return self.cmin


  def tabHash(self,nb, dim):
    tabHash0 = []
    for x in range(nb):
      vechash1 = []
      for y in range(dim):
	      nG = np.random.normal(0,1)
	      vechash1.append(nG)
      tabHash0.append(vechash1)
    return tabHash0



  def scaleRDD(self,rdd1):

    def insideRDD(size11,yy,minArray,maxArray):
      tabcoord = []
      for ind0 in range(size11):
        coordXi = (yy[1][ind0]-minArray[ind0])/(maxArray[ind0]-minArray[ind0])
        tabcoord.append(coordXi)
      return (yy[0],Vectors.dense(tabcoord))

    vecttest = rdd1.first()[1]
    size1 = len(vecttest)
    minArray = []
    maxArray = []

    for ind0 in range(size1):
      vectmin = rdd1.takeOrdered(1, key=lambda y: y[1][ind0])
      vectmax = rdd1.top(1, key=lambda y: y[1][ind0])
      min0 = vectmin[0][1][ind0]
      max0 = vectmax[0][1][ind0]
      minArray.append(min0)
      maxArray.append(max0)

    rdd2 = rdd1.map(lambda y: insideRDD(size1,y,minArray,maxArray))

    return (rdd2,maxArray,minArray)

  def descaleRDDcentroid(self,rdd1, maxMinArray0):
    vectest = rdd1.first()[1]
    size1 = len(vectest)
    maxArray = maxMinArray0[0]
    minArray = maxMinArray0[1]

    def f1(obj,maxArray,minArray):
      tabcoord = []
      for x in range(size1):
        coordXi = obj[1][x]*(maxArray[x]-minArray[x])+minArray[x]
        tabcoord.append(coordXi)
      return (obj[0], Vectors.dense(tabcoord))

    rdd2 = rdd1.map(lambda x: f1(x,maxArray,minArray))
    return rdd2

  def run(self,data, sc):
    data0 = sc.emptyRDD
    data.cache()
    size = int(data.count())
    dim = 0
    maxK = int(size/self.nbblocs) - 1

    if (size < self.cmin) : print("Exception : cmin > data size")  
    if (maxK <= self.k) : print("Exception : You set a too high K value")  

    if (self.normalisation) :
      parsedData00 = self.scaleRDD(data)
      data0 = parsedData00[0]
      maxMinArray = [parsedData00[1],parsedData00[2]] 
      dim = len(data0.first()[1])

    else :
      dim = len(data.first()[1])
      data0 = data

    hasher0 = LshHash()
    centroider0 = Bary0()

    ww = sc.broadcast(self.w)
    b = sc.broadcast(random.random() * self.w )
    tabHash0 = sc.broadcast(self.tabHash(self.nbseg,dim))
    hasher = sc.broadcast(hasher0)
    centroider = sc.broadcast(centroider0)

    def f1(obj):
      return (obj[0],obj[1],obj[1],hasher.value.hashfunc(obj[1],ww.value,b.value,tabHash0.value))

    rdd_LSH = data0.map(lambda x: f1(x)).repartition(self.nbblocs)
    rdd_res = sc.emptyRDD
    data.unpersist()

    rdd_LSH.cache()
    rdd_LSH.foreach(lambda x:self.void)

    def f21(obj1):
      array1 = list(obj1)
      def f3(obj2): 
        def f4(obj3):
          return list((obj3[1],obj2[1].squared_distance(obj3[1])))
        tab = list(map(f4,array1))
        tab = sorted(tab,key=lambda sd:sd[1])
        return list((obj2[0],obj2[1],centroider.value.bary(it.islice(tab,self.k),self.k)))
      return iter(list(map(f3,array1)))


    def f22(obj):
      return list((obj[0],obj[1],obj[2],hasher.value.hashfunc(obj[2],ww.value,b.value,tabHash0.value)))

    def f23(obj):
      return (obj[0],obj[2],obj[1])

    for ind in range(1,self.yStarIter+1):
      rdd_LSH_ord =  rdd_LSH.sortBy(lambda x: x[3]).mapPartitions(f21)
      if(ind < self.yStarIter):
        rdd_LSH_unpersist = rdd_LSH
        rdd_LSH = rdd_LSH_ord.map(f22)
        rdd_LSH.cache()
        rdd_LSH.foreach(self.void)
        rdd_LSH_unpersist.unpersist()
      else: rdd_res = rdd_LSH_ord.map(f23)


    ind1 = 0
    rdd_Ystar_ind = rdd_res.coalesce(self.numPart)
    rdd_Ystar_ind.cache()

    vector0 = rdd_Ystar_ind.first()[1]
    stop = size
    tab_ind = []
    rdd0 = sc.parallelize([["9999999",["9999999",Vectors.dense([1]),Vectors.dense([1])]]],1).filter(lambda x:x[0]=="1")

    def f21(obj):
      return (str(ind1),obj)

    def filter1(obj,vector,threshold):
      return obj[1].squared_distance(vector) <= threshold

    def printl(obj):
      print(obj)

    while ( stop != 0 ):
      
      # We mesure distance from Y* to others et we keep closest
      rdd_Clust_i_ind = rdd_Ystar_ind.filter( lambda x : filter1(x,vector0,self.threshold_cluster) ).cache()

      rdd_Clust_i2_ind = rdd_Clust_i_ind.map(f21)

      rdd_to_unpersist = rdd0
      rdd0 = rdd0.union(rdd_Clust_i2_ind).coalesce(self.numPart).cache()
      # Necessary action to keep rdd0 in memory beacause if RDD doens't persist data becomes corrupt and we get one cluster
      if ind1 % 20 == 19:
        rdd0.checkpoint()
      rdd0.foreach(lambda x:self.void)
      rdd_to_unpersist.unpersist()

      # We keep Y* whose distance is greather than threshold
      rdd_to_unpersist2 = rdd_Ystar_ind
      nbPart2 = int((stop/size)*self.numPart+1)
      rdd_Ystar_ind = rdd_Ystar_ind.subtract(rdd_Clust_i_ind,nbPart2).cache()
      if ind1 % 20 == 19:
        rdd_Ystar_ind.checkpoint()
      stop = int(rdd_Ystar_ind.count())
      rdd_to_unpersist2.unpersist()
      rdd_Clust_i_ind.unpersist()

      if(stop != 0) : vector0 = rdd_Ystar_ind.sortBy(lambda x:int(x[0])).first()[1]
      ind1 += 1 

    rdd_Ystar_ind.unpersist()

    '''
     Gives Y* labels to original data
    '''
    def f31(obj):
      return (obj[0],(obj[1][0],obj[1][2],obj[1][1]))

    rdd_Ystar_labeled = rdd0.map(lambda x:f31(x)).cache()
    #rdd_Ystar_labeled.sortBy(lambda x: int(x[1][0])).coalesce(1).saveAsTextFile("/home/kybe/Documents/iloan/P/ystarlabel")
    numElemByCLust0 = rdd_Ystar_labeled.countByKey()

    numElemByCLust = []    
    for key, value in numElemByCLust0.iteritems():
      temp = [int(key),value]
      numElemByCLust.append(temp)

    numElemByCLust = sorted(numElemByCLust,key=lambda elem:elem[0])

    def f32(obj):
      return (int(obj[0]),list(obj[1][1]))

    def reduce1(obj1,obj2):
      centroid = []
      for x in range(len(obj1)):
        centroid.append(obj1[x]+obj2[x])
      return centroid

    centroidArray = rdd_Ystar_labeled.map(f32).reduceByKey(lambda x,y:reduce1(x,y)).collect()
    centroidArray = sorted(centroidArray,key=lambda elem:elem[0])

    centroidArray1 = []
    rdd0.unpersist()

    # Form the array of clusters centroids
    for ind in range(len(numElemByCLust)):
      divedby = numElemByCLust[ind][1]
      def f4(obj):
        return obj/divedby
      centroidArray1.append((numElemByCLust[ind][0],Vectors.dense(list(map(f4,centroidArray[ind][1]))),ind)) 
    
    '''
     Fusion of cluster which cardinality is smaller than cmin 
    '''
    def filter2(obj):
      return obj[1] <= self.cmin

    tab_inf_cmin = filter(lambda elem: filter2(elem),numElemByCLust)
    stop_size = len(tab_inf_cmin)
    def f5(obj):
      return obj[0]
    tab_ind_petit = list(map(f5,tab_inf_cmin))
    map_ind_all = numElemByCLust0

    def f6(obj):
      return (obj[2],obj[0],obj[1],map_ind_all[str(obj[0])],obj[0])
    tabbar00 = list(map(f6,centroidArray1))
    tabbar00 = sorted(tabbar00,key=lambda onit:onit[0])
    tabbar01 = tabbar00

    def f7(obj):
      return (obj[2].squared_distance(tabbar01[cpt2][2]),obj[0],obj[1],obj[3])

    while(len(tab_ind_petit) != 0):
      for cpt2 in range(len(tabbar01)):
        if tabbar01[cpt2][3] < self.cmin:
          labelcurrent = tabbar01[cpt2][1]
          sizecurrent = tabbar01[cpt2][3]
          tabproche0 = list(map(f7,tabbar01))
          tabproche0 = sorted(tabproche0,key=lambda onit:onit[0])
          print "tabproche0"
          print list(map(lambda x:x[2],tabproche0))
          print "cardinality"
          print list(map(lambda x:x[3],tabproche0))
          cpt3 = 1
          while tabproche0[cpt3][2]==labelcurrent: cpt3 += 1
          plusproche = tabproche0[cpt3]
          labelplusproche = plusproche[2]
          sizeplusproche = plusproche[3]
          tab00 = list(filter(lambda x: x[1]==labelplusproche,tabbar01))
          tab01 = list(filter(lambda x: x[1]==labelcurrent,tabbar01))
          tabind0 = []
          #Update
          for ind8 in range(len(tab00)):
            tabind0.append(tab00[ind8][1])
            tabbar01[tab00[ind8][0]] = (tab00[ind8][0],labelplusproche,tab00[ind8][2],sizeplusproche+sizecurrent,tab00[ind8][4])
          for ind8 in range(len(tab01)):
            tabind0.append(tab01[ind8][1])
            tabbar01[tab01[ind8][0]] = (tab01[ind8][0],labelplusproche,tab01[ind8][2],sizeplusproche+sizecurrent,tab01[ind8][4])
          if sizecurrent+sizeplusproche >= self.cmin:
            tab_ind_petit = [x for x in tab_ind_petit if x not in tabind0]
        else:
          tab_ind_petit = [x for x in tab_ind_petit if x != tabbar01[cpt2][0]]

    tabbar000 = sc.broadcast(tabbar01)

    def f7(obj):
      cpt4 = 0
      while int(obj[0]) != int(tabbar000.value[cpt4][4]): cpt4 += 1
      return (tabbar000.value[cpt4][1],list((obj[1][0],obj[1][1])))

    rddf = rdd_Ystar_labeled.map(f7).cache()
    k0 = rddf.countByKey()
    numClust_Ystarer = len(k0)

    def f8(obj):
      return (obj[0],list(obj[1][1]))
    def f9(obj):
      def f10(obj1):
        return obj1/k0[obj[0]]
      print obj[1]
      return (obj[0],Vectors.dense(list(map(f10,obj[1]))))

    centroidF = rddf.map(f8).reduceByKey(lambda x,y:reduce1(x,y)).map(f9)
    centroidMap0 = self.descaleRDDcentroid(centroidF,maxMinArray).collect()

    centroidMap = {}
    for k,v in centroidMap0:
      centroidMap[k] = v

    rdd_Ystar_labeled.unpersist()

    return msmodel.Mean_shift_lsh_model(centroidMap,rddf,maxMinArray)
  
  '''
   Restore RDD original value
  '''
  def descaleRDD(self,rdd1,maxMinArray0) :
    vecttest = rdd1.first()[1][1]
    size1 = len(vecttest)
    maxArray = maxMinArray0[0]
    minArray = maxMinArray0[1]

    def f1(obj,maxArray,minArray):
      tabcoord = []
      for x in range(size1):
        coordXi = x[1][1][ind0]*(maxArray[ind0]-minArray[ind0])+minArray[ind0]
        tabcoord.append(coordXi)
      return list((obj[0],obj[1][0],Vectors.dense(tabcoord)))

    rdd2 = rdd1.map(lambda x: f1(x,maxArray,minArray))
    return rdd2



  def saveImageAnalysis(self, msmodel, folder,numpart=1) :
    rdd_final = self.descaleRDD(msmodel.rdd, msmodel.maxMinArray).map(lambda x:(int(x[1]),msmodel.clustersCenter[x[0]],x[0]))
    rdd_final.coalesce(numpart,true).sortBy(lambda x: x[0]).saveAsTextFile(folder)  
  

  '''
    Save clusters's label, cardinality and centroid
  '''
  def saveClusterInfo(self, sc1, msmodel, folder) :
    array1 = []    
    for key, value in msmodel.clustersCenter.iteritems():
      temp = [int(key),value]
      array1.append(temp)
    cardClust = msmodel.rdd.countByKey()
    rdd1 = sc1.parallelize(array1,1).map(lambda x: (x[0],cardClust[x[0]],x[1]))
    rdd1.sortBy(lambda x: int(x[0])).saveAsTextFile(folder)
  