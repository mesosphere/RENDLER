import hashlib
import wget

def dot(url_list, render_map, output_file):
  f = open(output_file, 'w')
  f.write("digraph G {\n")
  f.write("  node [shape=box];\n")

  # Download images
  urls_with_images = []
  for url in render_map:
    s3image_url = render_map[url]
    image_url = "http:" + s3image_url[3:]
    print "Downloading " + image_url
    filename = wget.download(image_url)
    url_hash = "X" + hashlib.sha256(url).hexdigest()
    f.write("  " + url_hash + "[label=\"\" image=\"" + filename + "\"];\n")
    urls_with_images.append(url_hash)

  for urls in url_list:
    (from_url, to_url) = urls

    from_hash = "X" + hashlib.sha256(from_url).hexdigest()
    to_hash = "X" + hashlib.sha256(to_url).hexdigest()

    if (from_hash not in urls_with_images):
      continue

    if (to_hash not in urls_with_images):
      continue

    f.write("  " + from_hash + " -> " + to_hash + ";\n")

  f.write("}\n")
  f.close()

  print "Results writting to " + output_file
  pass

dot([
    ("http://google.com", "http://github.com"),
    ("http://google.com", "http://yahoo.com")],
    {
      "http://google.com": "s3://26.media.tumblr.com/tumblr_lsmonudp4G1qchqb8o1_400.png",
      "http://github.com": "s3://26.media.tumblr.com/tumblr_lsmonudp4G1qchqb8o1_400.png",
      "http://yahoo.com":  "s3://26.media.tumblr.com/tumblr_lsmonudp4G1qchqb8o1_400.png",
    }, "test.dot")
