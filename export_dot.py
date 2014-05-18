def dot(url_list, render_map, output_file):
  f = open(output_file, 'w')
  f.write("digraph G {\n")
  f.write("  node [shape=box];\n")
  f.close()
  pass 

dot([
      ["http://google.com", "http://github.com"],
      ["http://google.com", "http://yahoo.com"]
    ],
    {
      "http://google.com": "google.png",
      "http://github.com": "github.png"
    },
    "test.dot")
