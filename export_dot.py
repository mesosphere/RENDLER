import hashlib
import wget

def dot(url_list, render_map, output_file):
  f = open(output_file, 'w')
  f.write("digraph G {\n")
  f.write("  node [shape=box];\n")

  # Download images
  for url in render_map:
    filename = wget.download(render_map[url])
    url_hash = "X" + hashlib.sha256(url).hexdigest()
    f.write("  " + url_hash + "[label=\"\" image=\"" + filename + "\"];\n")

  for urls in url_list:
    (from_url, to_url) = urls

    from_hash = "X" + hashlib.sha256(from_url).hexdigest()
    to_hash = "X" + hashlib.sha256(to_url).hexdigest()

    f.write("  " + from_hash + " -> " + to_hash + ";\n")

  f.write("}\n")
  f.close()

  print "Results writting to " + output_file
  pass

# dot([
#     ("http://google.com", "http://github.com"),
#     ("http://google.com", "http://yahoo.com")],
#     {
#       "http://google.com": "http://26.media.tumblr.com/tumblr_lsmonudp4G1qchqb8o1_400.png",
#       "http://github.com": "http://26.media.tumblr.com/tumblr_lsmonudp4G1qchqb8o1_400.png",
#       "http://yahoo.com":  "http://26.media.tumblr.com/tumblr_lsmonudp4G1qchqb8o1_400.png",
#     }, "test.dot")
