import json

class Result(object):
	def __repr__(self):
		return json.dumps(self.__dict__, sort_keys = True)

class CrawlResult(Result):
	"""The result of mining a resource for links

	CrawlResult must serialize to JSON as its default representation:

	>>> res = CrawlResult(
	... 	"1234",
	... 	"http://foo.co",
	... 	["http://foo.co/a", "http://foo.co/b"]
	... )
	>>> repr(res)
	'{"links": ["http://foo.co/a", "http://foo.co/b"], "taskId": "1234", "url": "http://foo.co"}'
	"""
	def __init__(self, taskId, url, links):
		self.taskId = taskId
		self.url    = url
		self.links  = links

class RenderResult(Result):
	"""The result of rendering a resource

	RenderResult must serialize to JSON as its default representation:

	>>> res = RenderResult(
	... 	"1234",
	... 	"http://foo.co",
	... 	"http://dl.mega.corp/foo.png"
	... )
	>>> repr(res)
	'{"imageUrl": "http://dl.mega.corp/foo.png", "taskId": "1234", "url": "http://foo.co"}'
	"""
	def __init__(self, taskId, url, imageUrl):
		self.taskId   = taskId
		self.url      = url
		self.imageUrl = imageUrl

if __name__ == "__main__":
    import doctest
    doctest.testmod()
