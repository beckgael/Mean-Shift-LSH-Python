class Mean_shift_lsh_model(object):
	"""docstring for mean_shift_lsh_model"""
	def __init__(self, clustersCenter, rdd, maxMinArray):
		self.clustersCenter = clustersCenter
		self.rdd = rdd
		self.maxMinArray = maxMinArray

	def getRDD(self):
		return self.rdd

	def getClusterCenter(self):
		return self.clustersCenter

	def getMaxMinArray(self):
		return self.maxMinArray

	def numCluster():
		return self.clustersCenter.size

	def predict(point):
		pass

	def predict(points):
		pass