import hashlib
import wget

def hash_url(url):
  return hashlib.sha256(url.encode('ascii', 'replace')).hexdigest()

def dot(url_list, render_map, output_file):
  print repr(render_map)
  f = open(output_file, 'w')
  f.write("digraph G {\n")
  f.write("  node [shape=box];\n")

  # Download images
  urls_with_images = []
  for url in render_map:
    image_url = render_map[url]

    if image_url[:8] == "file:///":
      # Support local runs. Format of url will be 'file:///<absolute path>'
      filename = image_url[8:]

    elif image_url[:5] == "s3://":
      # Support distributed runs uploading to Amazon S3.
      s3image_url = "http://" + image_url[5:]
      filename = wget.download(s3image_url)

    else:
      print "Don't know how to download " + image_url
      continue

    # Prepend character as dot vertices cannot starting with a digit.
    url_hash = "X" + hash_url(url)
    f.write("  " + url_hash + "[label=\"\" image=\"" + filename + "\"];\n")

    # Add to list as we sort out edges to vertices without an image.
    urls_with_images.append(url_hash)

  # Add vertices.
  for urls in url_list:
    (from_url, to_url) = urls

    from_hash = "X" + hash_url(from_url)
    to_hash = "X" + hash_url(to_url)

    if (from_hash not in urls_with_images):
      continue

    if (to_hash not in urls_with_images):
      continue

    # DOT format is:
    # A -> B;
    f.write("  " + from_hash + " -> " + to_hash + ";\n")

  f.write("}\n")
  f.close()

  print "Writing results to " + output_file
