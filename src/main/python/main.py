from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg import Vectors
import msmodel
import ms

conf = SparkConf().setAppName("DNNMS-LSH")#.setMaster("master")
sc = SparkContext(conf=conf)

data = sc.textFile("myPathTomyfile/myfile.csv")

def f(obj):
	l = []
	l.append(obj[0])
	l.append(Vectors.dense(map(lambda y:float(y),obj[1:])))
	return l

parseddata = data.map(lambda x: x.split(",")).map(lambda x:f(x))


ms1 = ms.msLsh(
				k=5,
				threshold_cluster=0.005,
				yStarIter=1,
				cmin=40,
				normalisation=True,
				w=1,
				nbseg=100,
				nbblocs=1,
				numPart=6
			)


msmodel = ms1.run(parseddata,sc)

print msmodel.clustersCenter